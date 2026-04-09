"""
Regenera el JSON de análisis incluyendo datos de población y áreas sanitarias.
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
                   COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales_unicos,
                   COUNT(DISTINCT centro) as centros
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
            SELECT especialidad_normalizada, COUNT(*) as n_asisa
            FROM cuadro_medico
            WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL AND profesional != ''
            GROUP BY especialidad_normalizada
        ),
        competencia AS (
            SELECT especialidad_normalizada,
                   ROUND(AVG(cnt), 1) as promedio_competencia,
                   MAX(cnt) as max_competencia,
                   GROUP_CONCAT(aseguradora || ':' || cnt) as detalle
            FROM (
                SELECT aseguradora, especialidad_normalizada, COUNT(*) as cnt
                FROM cuadro_medico
                WHERE aseguradora != 'ASISA' AND especialidad_normalizada IS NOT NULL AND profesional != ''
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

    results['especialidades_por_aseguradora'] = query(conn, '''
        SELECT aseguradora, especialidad_normalizada, COUNT(*) as profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND profesional != ''
        GROUP BY aseguradora, especialidad_normalizada
        ORDER BY aseguradora, profesionales DESC
    ''')

    # 3. GAPS ESPECIALIDADES
    results['gaps_especialidades_asisa'] = query(conn, '''
        SELECT especialidad_normalizada, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras_que_la_tienen,
               SUM(CASE WHEN profesional != '' THEN 1 ELSE 0 END) as total_profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL
          AND especialidad_normalizada NOT IN (
              SELECT DISTINCT especialidad_normalizada FROM cuadro_medico
              WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL
          )
        GROUP BY especialidad_normalizada
        ORDER BY total_profesionales DESC
    ''')

    # 4. COBERTURA POR MUNICIPIO
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
               COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales,
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
        SELECT municipio, especialidad_normalizada,
               aseguradoras_con_cobertura,
               CASE WHEN asisa_tiene > 0 THEN 'SI' ELSE 'NO' END as asisa_cubre
        FROM mercado
        WHERE asisa_tiene = 0 AND aseguradoras_con_cobertura >= 2
        ORDER BY aseguradoras_con_cobertura DESC, municipio
    ''')

    # 7. CENTROS
    results['gaps_centros_asisa'] = query(conn, '''
        SELECT centro, municipio,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               COUNT(DISTINCT especialidad_normalizada) as especialidades,
               COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND centro != ''
          AND centro NOT IN (
              SELECT DISTINCT centro FROM cuadro_medico WHERE aseguradora = 'ASISA' AND centro != ''
          )
        GROUP BY centro, municipio HAVING profesionales > 2
        ORDER BY profesionales DESC
    ''')

    results['concentracion_centros'] = query(conn, '''
        SELECT aseguradora, centro, municipio, COUNT(*) as registros,
               COUNT(DISTINCT especialidad_normalizada) as especialidades
        FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL AND centro != ''
        GROUP BY aseguradora, centro ORDER BY aseguradora, registros DESC
    ''')

    # 8. PROFESIONALES
    results['profesionales_compartidos'] = query(conn, '''
        SELECT profesional, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               COUNT(DISTINCT aseguradora) as num_aseguradoras
        FROM cuadro_medico
        WHERE profesional != '' AND especialidad_normalizada IS NOT NULL
          AND profesional IN (
              SELECT profesional FROM cuadro_medico WHERE aseguradora = 'ASISA' AND profesional != ''
          )
        GROUP BY profesional HAVING COUNT(DISTINCT aseguradora) > 1
        ORDER BY num_aseguradoras DESC
    ''')

    results['profesionales_exclusivos_competencia'] = query(conn, '''
        SELECT profesional, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               GROUP_CONCAT(DISTINCT municipio) as municipios
        FROM cuadro_medico
        WHERE profesional != '' AND especialidad_normalizada IS NOT NULL
          AND profesional NOT IN (
              SELECT DISTINCT profesional FROM cuadro_medico WHERE aseguradora = 'ASISA' AND profesional != ''
          )
        GROUP BY profesional HAVING COUNT(DISTINCT aseguradora) >= 2
        ORDER BY COUNT(DISTINCT aseguradora) DESC LIMIT 100
    ''')

    # 9. KPIs COMPETITIVOS
    all_specs = query(conn, 'SELECT COUNT(DISTINCT especialidad_normalizada) as total FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL')[0]['total']
    asisa_specs = query(conn, "SELECT COUNT(DISTINCT especialidad_normalizada) as total FROM cuadro_medico WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL")[0]['total']

    results['kpis_competitivos'] = {
        'especialidades_mercado': all_specs,
        'especialidades_asisa': asisa_specs,
        'pct_cobertura_especialidades': round(asisa_specs / all_specs * 100, 1) if all_specs else 0,
        'por_aseguradora': query(conn, '''
            SELECT aseguradora,
                   COUNT(DISTINCT especialidad_normalizada) as especialidades,
                   COUNT(DISTINCT municipio) as municipios,
                   COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales,
                   COUNT(DISTINCT centro) as centros
            FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL
            GROUP BY aseguradora
        '''),
    }

    # 10. NUEVO: POR AREA SANITARIA
    results['areas_sanitarias'] = query(conn, '''
        SELECT area_sanitaria,
               MAX(poblacion_area) as poblacion,
               SUM(CASE WHEN aseguradora = 'ASISA' THEN 1 ELSE 0 END) as asisa_registros,
               SUM(CASE WHEN aseguradora != 'ASISA' THEN 1 ELSE 0 END) as competencia_registros,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' THEN profesional END) as asisa_profesionales,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' THEN profesional END) as comp_profesionales,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' THEN especialidad_normalizada END) as asisa_especialidades,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' THEN especialidad_normalizada END) as comp_especialidades,
               COUNT(DISTINCT municipio) as municipios,
               COUNT(DISTINCT aseguradora) as aseguradoras
        FROM cuadro_medico
        WHERE area_sanitaria IS NOT NULL AND especialidad_normalizada IS NOT NULL
        GROUP BY area_sanitaria
        ORDER BY poblacion DESC
    ''')

    # 11. NUEVO: RATIOS POR POBLACION (por area sanitaria)
    results['ratios_por_area'] = query(conn, '''
        SELECT area_sanitaria,
               MAX(poblacion_area) as poblacion,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional != '' THEN profesional END) as asisa_prof,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional != '' THEN profesional END) * 10000.0 / MAX(poblacion_area), 2) as asisa_prof_per_10k,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' AND profesional != '' THEN profesional END) * 10000.0 / MAX(poblacion_area) / 4.0, 2) as comp_avg_prof_per_10k
        FROM cuadro_medico
        WHERE area_sanitaria IS NOT NULL AND especialidad_normalizada IS NOT NULL
        GROUP BY area_sanitaria
        ORDER BY asisa_prof_per_10k ASC
    ''')

    # 12. NUEVO: DENSIDAD POR MUNICIPIO PRINCIPAL
    results['densidad_municipios'] = query(conn, '''
        SELECT municipio,
               MAX(CAST(poblacion_municipio AS INTEGER)) as poblacion,
               MAX(area_sanitaria) as area_sanitaria,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional != '' THEN profesional END) as asisa_prof,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' AND profesional != '' THEN profesional END) as comp_prof,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' AND profesional != '' THEN profesional END) * 10000.0 / MAX(CAST(poblacion_municipio AS INTEGER)), 2) as asisa_per_10k,
               ROUND(COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' AND profesional != '' THEN profesional END) * 10000.0 / MAX(CAST(poblacion_municipio AS INTEGER)) / 4.0, 2) as comp_avg_per_10k
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
    print(f'JSON guardado: {OUTPUT_JSON} ({os.path.getsize(OUTPUT_JSON):,} bytes)')
    print(f'Dashboard JSON: {DASHBOARD_JSON}')

    # Key stats
    print('\nRatios ASISA por area sanitaria:')
    for r in results['ratios_por_area']:
        print(f"  {r['area_sanitaria']}: {r['asisa_prof']} prof / {r['poblacion']} hab = {r['asisa_prof_per_10k']} per 10K (comp avg: {r['comp_avg_prof_per_10k']})")

if __name__ == '__main__':
    run_analysis()
