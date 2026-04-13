"""
Análisis v2: usa las columnas normalizadas (centro_norm, profesional_norm) para matching correcto.
Añade:
- Profesionales que la competencia tiene y ASISA no
- Profesionales exclusivos de ASISA
- Corrige gaps de centros (ya no falla por diferencias de mayúsculas/acentos)
"""
import sqlite3
import json
import os

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')
OUTPUT_JSON = os.path.join(BASE, 'data', 'analysis_results.json')
DASHBOARD_JSON = os.path.join(BASE, 'dashboard', 'data.json')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def query(conn, sql, params=()):
    return [dict(r) for r in conn.execute(sql, params).fetchall()]

def run_analysis():
    conn = get_conn()
    results = {}

    # 1. RESUMEN GENERAL
    results['resumen'] = {
        'por_aseguradora': query(conn, '''
            SELECT aseguradora,
                   COUNT(*) as total_registros,
                   COUNT(DISTINCT especialidad_normalizada) as especialidades,
                   COUNT(DISTINCT municipio) as municipios,
                   COUNT(DISTINCT CASE WHEN profesional_norm != '' THEN profesional_norm END) as profesionales_unicos,
                   COUNT(DISTINCT CASE WHEN centro_norm != '' THEN centro_norm END) as centros
            FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL
            GROUP BY aseguradora ORDER BY total_registros DESC
        '''),
        'totales': query(conn, '''
            SELECT COUNT(*) as registros,
                   COUNT(DISTINCT especialidad_normalizada) as especialidades,
                   COUNT(DISTINCT municipio) as municipios
            FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL
        ''')[0],
    }

    # 2. ESPECIALIDADES COMPARATIVA
    results['comparativa_especialidades'] = query(conn, '''
        WITH asisa AS (
            SELECT especialidad_normalizada, COUNT(DISTINCT profesional_norm) as n_asisa
            FROM cuadro_medico
            WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL AND profesional_norm != ''
            GROUP BY especialidad_normalizada
        ),
        competencia AS (
            SELECT especialidad_normalizada,
                   ROUND(AVG(cnt), 1) as promedio_competencia,
                   MAX(cnt) as max_competencia,
                   GROUP_CONCAT(aseguradora || ':' || cnt) as detalle
            FROM (
                SELECT aseguradora, especialidad_normalizada, COUNT(DISTINCT profesional_norm) as cnt
                FROM cuadro_medico
                WHERE aseguradora != 'ASISA' AND especialidad_normalizada IS NOT NULL AND profesional_norm != ''
                GROUP BY aseguradora, especialidad_normalizada
            )
            GROUP BY especialidad_normalizada
        )
        SELECT COALESCE(a.especialidad_normalizada, c.especialidad_normalizada) as especialidad,
               COALESCE(a.n_asisa, 0) as asisa,
               COALESCE(c.promedio_competencia, 0) as promedio_competencia,
               COALESCE(c.max_competencia, 0) as max_competencia,
               COALESCE(a.n_asisa, 0) - COALESCE(c.promedio_competencia, 0) as diferencia_vs_promedio,
               c.detalle
        FROM asisa a
        FULL OUTER JOIN competencia c ON a.especialidad_normalizada = c.especialidad_normalizada
        ORDER BY diferencia_vs_promedio ASC
    ''')

    # 3. GAPS ESPECIALIDADES
    results['gaps_especialidades_asisa'] = query(conn, '''
        SELECT especialidad_normalizada, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras_que_la_tienen,
               COUNT(DISTINCT profesional_norm) as total_profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL
          AND especialidad_normalizada NOT IN (
              SELECT DISTINCT especialidad_normalizada FROM cuadro_medico
              WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL
          )
        GROUP BY especialidad_normalizada
        ORDER BY total_profesionales DESC
    ''')

    # 4. RANKING MUNICIPIOS
    results['ranking_municipios'] = query(conn, '''
        SELECT municipio,
               MAX(poblacion_municipio) as poblacion,
               MAX(area_sanitaria) as area_sanitaria,
               SUM(CASE WHEN aseguradora = 'ASISA' THEN 1 ELSE 0 END) as asisa_registros,
               SUM(CASE WHEN aseguradora != 'ASISA' THEN 1 ELSE 0 END) as competencia_registros,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' THEN especialidad_normalizada END) as asisa_especialidades,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' THEN especialidad_normalizada END) as competencia_especialidades,
               COUNT(DISTINCT aseguradora) as num_aseguradoras
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND municipio != ''
        GROUP BY municipio
        ORDER BY competencia_registros DESC
    ''')

    # 5. GAPS MUNICIPIOS
    results['gaps_municipios_asisa'] = query(conn, '''
        SELECT municipio,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras_presentes,
               COUNT(DISTINCT especialidad_normalizada) as especialidades_disponibles,
               COUNT(DISTINCT profesional_norm) as profesionales,
               MAX(poblacion_municipio) as poblacion
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND municipio != ''
          AND municipio NOT IN (
              SELECT DISTINCT municipio FROM cuadro_medico WHERE aseguradora = 'ASISA' AND municipio != ''
          )
        GROUP BY municipio ORDER BY profesionales DESC
    ''')

    # 6. GAPS CRITICOS
    results['gaps_criticos'] = query(conn, '''
        WITH mercado AS (
            SELECT municipio, especialidad_normalizada,
                   COUNT(DISTINCT aseguradora) as aseguradoras_con_cobertura,
                   SUM(CASE WHEN aseguradora = 'ASISA' THEN 1 ELSE 0 END) as asisa_tiene
            FROM cuadro_medico
            WHERE especialidad_normalizada IS NOT NULL AND municipio != ''
            GROUP BY municipio, especialidad_normalizada
        )
        SELECT municipio, especialidad_normalizada, aseguradoras_con_cobertura
        FROM mercado
        WHERE asisa_tiene = 0 AND aseguradoras_con_cobertura >= 2
        ORDER BY aseguradoras_con_cobertura DESC, municipio
    ''')

    # ============================================================
    # 7. CENTROS - CORREGIDO con normalización
    # ============================================================

    # Set de centros donde ASISA tiene presencia (normalizado)
    asisa_centers = set()
    cur = conn.execute("SELECT DISTINCT centro_norm FROM cuadro_medico WHERE aseguradora = 'ASISA' AND centro_norm != ''")
    for row in cur.fetchall():
        asisa_centers.add(row[0])

    # 7a. Centros donde competencia tiene presencia y ASISA NO (matching correcto)
    # Excluir nombres genéricos que no son centros reales
    generic_center_names = ['consultorio', 'consulta', 'centro medico', 'clinica', 'centro']
    results['gaps_centros_asisa'] = query(conn, '''
        SELECT centro_norm, MAX(centro) as centro_original,
               municipio,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               COUNT(DISTINCT especialidad_normalizada) as especialidades,
               COUNT(DISTINCT profesional_norm) as profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL
          AND centro_norm != ''
          AND centro_norm NOT IN ('consultorio', 'consulta', 'centro medico', 'clinica', 'centro', 'consulta privada')
          AND LENGTH(centro_norm) > 4
          AND centro_norm NOT IN (
              SELECT DISTINCT centro_norm FROM cuadro_medico
              WHERE aseguradora = 'ASISA' AND centro_norm != ''
          )
        GROUP BY centro_norm, municipio
        HAVING profesionales >= 2
        ORDER BY profesionales DESC
    ''')

    # 7b. Centros compartidos ASISA + competencia (para validación)
    results['centros_compartidos'] = query(conn, '''
        SELECT centro_norm,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               COUNT(DISTINCT aseguradora) as num_aseguradoras,
               COUNT(DISTINCT profesional_norm) as profesionales_totales,
               MAX(municipio) as municipio
        FROM cuadro_medico
        WHERE centro_norm != ''
          AND centro_norm IN (
              SELECT DISTINCT centro_norm FROM cuadro_medico WHERE aseguradora = 'ASISA' AND centro_norm != ''
          )
        GROUP BY centro_norm
        HAVING COUNT(DISTINCT aseguradora) >= 2
        ORDER BY num_aseguradoras DESC, profesionales_totales DESC
    ''')

    # 7c. Concentración por centro
    results['concentracion_centros'] = query(conn, '''
        SELECT aseguradora, centro_norm, MAX(centro) as centro_original,
               MAX(municipio) as municipio,
               COUNT(DISTINCT profesional_norm) as profesionales,
               COUNT(DISTINCT especialidad_normalizada) as especialidades
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND centro_norm != ''
        GROUP BY aseguradora, centro_norm
        ORDER BY aseguradora, profesionales DESC
    ''')

    # ============================================================
    # 8. PROFESIONALES - NUEVAS VISTAS
    # ============================================================

    # 8a. Profesionales compartidos ASISA + competencia
    results['profesionales_compartidos'] = query(conn, '''
        SELECT profesional_norm,
               MAX(CASE WHEN aseguradora = 'ASISA' THEN profesional END) as nombre_asisa,
               MAX(CASE WHEN aseguradora != 'ASISA' THEN profesional END) as nombre_competencia,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               MAX(municipio) as municipio,
               COUNT(DISTINCT aseguradora) as num_aseguradoras
        FROM cuadro_medico
        WHERE profesional_norm != '' AND especialidad_normalizada IS NOT NULL
          AND profesional_norm IN (
              SELECT profesional_norm FROM cuadro_medico WHERE aseguradora = 'ASISA' AND profesional_norm != ''
          )
        GROUP BY profesional_norm
        HAVING COUNT(DISTINCT aseguradora) > 1
        ORDER BY num_aseguradoras DESC, profesional_norm
    ''')

    # 8b. NUEVO: Profesionales que competencia tiene y ASISA NO
    results['profesionales_faltantes_asisa'] = query(conn, '''
        SELECT profesional_norm,
               MAX(profesional) as nombre,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               MAX(municipio) as municipio,
               MAX(centro) as centro,
               COUNT(DISTINCT aseguradora) as num_competidores
        FROM cuadro_medico
        WHERE profesional_norm != ''
          AND especialidad_normalizada IS NOT NULL
          AND aseguradora != 'ASISA'
          AND profesional_norm NOT IN (
              SELECT DISTINCT profesional_norm FROM cuadro_medico
              WHERE aseguradora = 'ASISA' AND profesional_norm != ''
          )
        GROUP BY profesional_norm
        ORDER BY num_competidores DESC, profesional_norm
    ''')

    # 8c. NUEVO: Profesionales exclusivos de ASISA
    results['profesionales_exclusivos_asisa'] = query(conn, '''
        SELECT profesional_norm,
               MAX(profesional) as nombre,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               MAX(municipio) as municipio,
               MAX(centro) as centro
        FROM cuadro_medico
        WHERE profesional_norm != ''
          AND especialidad_normalizada IS NOT NULL
          AND aseguradora = 'ASISA'
          AND profesional_norm NOT IN (
              SELECT DISTINCT profesional_norm FROM cuadro_medico
              WHERE aseguradora != 'ASISA' AND profesional_norm != ''
          )
        GROUP BY profesional_norm
        ORDER BY profesional_norm
    ''')

    # ============================================================
    # 9. KPIs COMPETITIVOS
    # ============================================================
    all_specs = query(conn, 'SELECT COUNT(DISTINCT especialidad_normalizada) as total FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL')[0]['total']
    asisa_specs = query(conn, "SELECT COUNT(DISTINCT especialidad_normalizada) as total FROM cuadro_medico WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL")[0]['total']

    results['kpis_competitivos'] = {
        'especialidades_mercado': all_specs,
        'especialidades_asisa': asisa_specs,
        'pct_cobertura_especialidades': round(asisa_specs / all_specs * 100, 1) if all_specs else 0,
        'profesionales_compartidos_total': len(results['profesionales_compartidos']),
        'profesionales_faltantes_total': len(results['profesionales_faltantes_asisa']),
        'profesionales_exclusivos_asisa_total': len(results['profesionales_exclusivos_asisa']),
        'por_aseguradora': query(conn, '''
            SELECT aseguradora,
                   COUNT(DISTINCT especialidad_normalizada) as especialidades,
                   COUNT(DISTINCT municipio) as municipios,
                   COUNT(DISTINCT CASE WHEN profesional_norm != '' THEN profesional_norm END) as profesionales,
                   COUNT(DISTINCT CASE WHEN centro_norm != '' THEN centro_norm END) as centros
            FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL
            GROUP BY aseguradora
        '''),
    }

    # 10. AREAS SANITARIAS
    results['areas_sanitarias'] = query(conn, '''
        SELECT area_sanitaria,
               MAX(poblacion_area) as poblacion,
               SUM(CASE WHEN aseguradora = 'ASISA' THEN 1 ELSE 0 END) as asisa_registros,
               SUM(CASE WHEN aseguradora != 'ASISA' THEN 1 ELSE 0 END) as competencia_registros,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' THEN profesional_norm END) as asisa_profesionales,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' THEN profesional_norm END) as comp_profesionales,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' THEN especialidad_normalizada END) as asisa_especialidades,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' THEN especialidad_normalizada END) as comp_especialidades,
               COUNT(DISTINCT municipio) as municipios,
               COUNT(DISTINCT aseguradora) as aseguradoras
        FROM cuadro_medico
        WHERE area_sanitaria IS NOT NULL AND especialidad_normalizada IS NOT NULL
        GROUP BY area_sanitaria
        ORDER BY poblacion DESC
    ''')

    # 11. RATIOS POR AREA
    results['ratios_por_area'] = query(conn, '''
        SELECT area_sanitaria,
               MAX(poblacion_area) as poblacion,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional_norm != '' THEN profesional_norm END) as asisa_prof,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional_norm != '' THEN profesional_norm END) * 10000.0 / MAX(poblacion_area), 2) as asisa_prof_per_10k,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' AND profesional_norm != '' THEN profesional_norm END) * 10000.0 / MAX(poblacion_area) / 4.0, 2) as comp_avg_prof_per_10k
        FROM cuadro_medico
        WHERE area_sanitaria IS NOT NULL AND especialidad_normalizada IS NOT NULL
        GROUP BY area_sanitaria
        ORDER BY asisa_prof_per_10k ASC
    ''')

    # 12. DENSIDAD MUNICIPIOS
    results['densidad_municipios'] = query(conn, '''
        SELECT municipio,
               MAX(CAST(poblacion_municipio AS INTEGER)) as poblacion,
               MAX(area_sanitaria) as area_sanitaria,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional_norm != '' THEN profesional_norm END) as asisa_prof,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' AND profesional_norm != '' THEN profesional_norm END) as comp_prof,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional_norm != '' THEN profesional_norm END) * 10000.0 / MAX(CAST(poblacion_municipio AS INTEGER)), 2) as asisa_per_10k,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' AND profesional_norm != '' THEN profesional_norm END) * 10000.0 / MAX(CAST(poblacion_municipio AS INTEGER)) / 4.0, 2) as comp_avg_per_10k
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND municipio != '' AND poblacion_municipio IS NOT NULL
            AND CAST(poblacion_municipio AS INTEGER) > 0
        GROUP BY municipio
        HAVING MAX(CAST(poblacion_municipio AS INTEGER)) > 3000
        ORDER BY asisa_per_10k ASC
    ''')

    # SAVE
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(DASHBOARD_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    conn.close()
    print(f'JSON guardado: {OUTPUT_JSON}')
    print(f'Dashboard JSON: {DASHBOARD_JSON}')

    # REPORT
    print('\n=== HALLAZGOS CON MATCHING NORMALIZADO ===\n')
    print(f'Centros donde ASISA no esta (vs competencia): {len(results["gaps_centros_asisa"])}')
    print(f'Centros compartidos (ASISA + 1+ competidor): {len(results["centros_compartidos"])}')
    print()
    print(f'Profesionales compartidos (ASISA + 1+ competidor): {len(results["profesionales_compartidos"])}')
    print(f'Profesionales que competencia tiene y ASISA no: {len(results["profesionales_faltantes_asisa"])}')
    print(f'Profesionales exclusivos de ASISA: {len(results["profesionales_exclusivos_asisa"])}')

    print('\nTop 15 centros donde ASISA no esta:')
    for g in results['gaps_centros_asisa'][:15]:
        print(f'  {g["centro_original"]:45s} | {g["municipio"]:20s} | {g["aseguradoras"]:30s} | {g["profesionales"]} prof')

    print('\nCentros compartidos (sample):')
    for c in results['centros_compartidos'][:10]:
        print(f'  {c["centro_norm"]:40s} | {c["aseguradoras"]:30s}')

if __name__ == '__main__':
    run_analysis()
