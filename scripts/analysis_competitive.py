"""
Análisis competitivo: ASISA vs mercado en Asturias.
Genera JSON con todos los KPIs para el dashboard.
"""
import sqlite3
import json
import os

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')
OUTPUT_JSON = os.path.join(BASE, 'data', 'analysis_results.json')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def query(conn, sql, params=()):
    return [dict(r) for r in conn.execute(sql, params).fetchall()]

def run_analysis():
    conn = get_conn()
    results = {}

    # ============================================================
    # 1. RESUMEN GENERAL
    # ============================================================
    results['resumen'] = {
        'por_aseguradora': query(conn, '''
            SELECT aseguradora,
                   COUNT(*) as total_registros,
                   COUNT(DISTINCT especialidad_normalizada) as especialidades,
                   COUNT(DISTINCT municipio) as municipios,
                   COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales_unicos,
                   COUNT(DISTINCT centro) as centros
            FROM cuadro_medico
            WHERE especialidad_normalizada IS NOT NULL
            GROUP BY aseguradora ORDER BY total_registros DESC
        '''),
        'totales': query(conn, '''
            SELECT COUNT(*) as registros,
                   COUNT(DISTINCT especialidad_normalizada) as especialidades,
                   COUNT(DISTINCT municipio) as municipios
            FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL
        ''')[0],
    }

    # ============================================================
    # 2. ESPECIALIDADES: ASISA vs COMPETENCIA
    # ============================================================

    # 2a. Especialidades por aseguradora
    results['especialidades_por_aseguradora'] = query(conn, '''
        SELECT aseguradora, especialidad_normalizada, COUNT(*) as profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND profesional != ''
        GROUP BY aseguradora, especialidad_normalizada
        ORDER BY aseguradora, profesionales DESC
    ''')

    # 2b. Especialidades que tiene la competencia y ASISA NO
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

    # 2c. Comparativa de profesionales por especialidad: ASISA vs promedio competencia
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

    # ============================================================
    # 3. COBERTURA POR MUNICIPIO
    # ============================================================

    # 3a. Profesionales por municipio y aseguradora
    results['cobertura_municipio'] = query(conn, '''
        SELECT municipio, aseguradora,
               COUNT(*) as registros,
               COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales,
               COUNT(DISTINCT especialidad_normalizada) as especialidades
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND municipio != ''
        GROUP BY municipio, aseguradora
        ORDER BY municipio, registros DESC
    ''')

    # 3b. Municipios donde ASISA no tiene presencia pero otros sí
    results['gaps_municipios_asisa'] = query(conn, '''
        SELECT municipio,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras_presentes,
               COUNT(DISTINCT especialidad_normalizada) as especialidades_disponibles,
               COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL
          AND municipio != ''
          AND municipio NOT IN (
              SELECT DISTINCT municipio FROM cuadro_medico
              WHERE aseguradora = 'ASISA' AND municipio != ''
          )
        GROUP BY municipio
        ORDER BY profesionales DESC
    ''')

    # 3c. Ranking de municipios: ASISA vs competencia (profesionales)
    results['ranking_municipios'] = query(conn, '''
        SELECT municipio,
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

    # ============================================================
    # 4. CENTROS: ASISA vs COMPETENCIA
    # ============================================================

    # 4a. Centros donde la competencia tiene presencia y ASISA no
    results['gaps_centros_asisa'] = query(conn, '''
        SELECT centro, municipio,
               GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               COUNT(DISTINCT especialidad_normalizada) as especialidades,
               COUNT(DISTINCT CASE WHEN profesional != '' THEN profesional END) as profesionales
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL
          AND centro != ''
          AND centro NOT IN (
              SELECT DISTINCT centro FROM cuadro_medico WHERE aseguradora = 'ASISA' AND centro != ''
          )
        GROUP BY centro, municipio
        HAVING profesionales > 2
        ORDER BY profesionales DESC
    ''')

    # 4b. Concentración: top centros por aseguradora
    results['concentracion_centros'] = query(conn, '''
        SELECT aseguradora, centro, municipio, COUNT(*) as registros,
               COUNT(DISTINCT especialidad_normalizada) as especialidades
        FROM cuadro_medico
        WHERE especialidad_normalizada IS NOT NULL AND centro != ''
        GROUP BY aseguradora, centro
        ORDER BY aseguradora, registros DESC
    ''')

    # ============================================================
    # 5. PROFESIONALES COMPARTIDOS
    # ============================================================

    # 5a. Profesionales en competencia que NO están en ASISA
    results['profesionales_exclusivos_competencia'] = query(conn, '''
        SELECT profesional, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               GROUP_CONCAT(DISTINCT municipio) as municipios
        FROM cuadro_medico
        WHERE profesional != ''
          AND especialidad_normalizada IS NOT NULL
          AND profesional NOT IN (
              SELECT DISTINCT profesional FROM cuadro_medico
              WHERE aseguradora = 'ASISA' AND profesional != ''
          )
        GROUP BY profesional
        HAVING COUNT(DISTINCT aseguradora) >= 2
        ORDER BY COUNT(DISTINCT aseguradora) DESC
        LIMIT 100
    ''')

    # 5b. Profesionales compartidos entre ASISA y competencia
    results['profesionales_compartidos'] = query(conn, '''
        SELECT profesional, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               COUNT(DISTINCT aseguradora) as num_aseguradoras
        FROM cuadro_medico
        WHERE profesional != '' AND especialidad_normalizada IS NOT NULL
          AND profesional IN (
              SELECT profesional FROM cuadro_medico WHERE aseguradora = 'ASISA' AND profesional != ''
          )
        GROUP BY profesional
        HAVING COUNT(DISTINCT aseguradora) > 1
        ORDER BY num_aseguradoras DESC
    ''')

    # ============================================================
    # 6. KPIs COMPETITIVOS
    # ============================================================

    # 6a. % especialidades cubiertas ASISA vs total mercado
    all_specs = query(conn, '''
        SELECT COUNT(DISTINCT especialidad_normalizada) as total
        FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL
    ''')[0]['total']
    asisa_specs = query(conn, '''
        SELECT COUNT(DISTINCT especialidad_normalizada) as total
        FROM cuadro_medico WHERE aseguradora = 'ASISA' AND especialidad_normalizada IS NOT NULL
    ''')[0]['total']

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
            FROM cuadro_medico
            WHERE especialidad_normalizada IS NOT NULL
            GROUP BY aseguradora
        '''),
    }

    # 6b. Gaps críticos por zona y especialidad
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

    # ============================================================
    # SAVE
    # ============================================================
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    conn.close()
    print(f'Análisis guardado: {OUTPUT_JSON}')
    print(f'Tamaño: {os.path.getsize(OUTPUT_JSON):,} bytes')

    # Print key findings
    print('\n' + '='*60)
    print('HALLAZGOS CLAVE')
    print('='*60)

    print(f'\n📊 COBERTURA DE ESPECIALIDADES')
    print(f'  Mercado total: {all_specs} especialidades')
    print(f'  ASISA cubre: {asisa_specs} ({round(asisa_specs/all_specs*100,1)}%)')
    gaps = results['gaps_especialidades_asisa']
    if gaps:
        print(f'  Especialidades que ASISA NO tiene ({len(gaps)}):')
        for g in gaps:
            print(f'    - {g["especialidad_normalizada"]} (la tienen: {g["aseguradoras_que_la_tienen"]})')

    print(f'\n🏥 GAPS EN MUNICIPIOS')
    muni_gaps = results['gaps_municipios_asisa']
    if muni_gaps:
        print(f'  Municipios donde ASISA no está ({len(muni_gaps)}):')
        for g in muni_gaps[:15]:
            print(f'    - {g["municipio"]} ({g["profesionales"]} profesionales, aseguradoras: {g["aseguradoras_presentes"]})')

    print(f'\n🏗️ GAPS CRÍTICOS (especialidad+municipio que otros tienen y ASISA no)')
    criticos = results['gaps_criticos']
    print(f'  Total combinaciones zona+especialidad sin cubrir: {len(criticos)}')
    if criticos:
        print(f'  Más críticos (cubiertos por 2+ competidores):')
        for g in criticos[:20]:
            print(f'    - {g["municipio"]} / {g["especialidad_normalizada"]} ({g["aseguradoras_con_cobertura"]} competidores)')

    print(f'\n👥 PROFESIONALES')
    comp = results['comparativa_especialidades']
    print(f'  Especialidades donde ASISA está POR DEBAJO del promedio:')
    for c in comp:
        if c['diferencia_vs_promedio'] and c['diferencia_vs_promedio'] < -3:
            print(f'    - {c["especialidad"]}: ASISA={c["asisa"]}, promedio competencia={c["promedio_competencia"]}')

    shared = results['profesionales_compartidos']
    print(f'\n  Profesionales compartidos ASISA+competencia: {len(shared)}')

    return results

if __name__ == '__main__':
    run_analysis()
