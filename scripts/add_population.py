"""
Añade datos de población por municipio y áreas sanitarias SESPA al SQLite.
Fuentes: INE 2024, SESPA (8 áreas actuales).
"""
import sqlite3
import json
import os

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')

# INE 2024 population data
POPULATION = {
    'Gijón': 268313,
    'Oviedo': 217584,
    'Avilés': 75518,
    'Siero': 52194,
    'Pola de Siero': 52194,  # Same municipality
    'Langreo': 37978,
    'Mieres': 36195,
    'Castrillón': 22103,
    'Piedras Blancas': 22103,  # Capital of Castrillón
    'Villaviciosa': 15000,
    'Llanera': 13948,
    'Lugones': 13948,  # Capital of Llanera
    'Llanes': 13524,
    'Laviana': 12355,
    'Lena': 12545,
    'Pola de Lena': 12545,
    'Aller': 12324,
    'Cangas del Narcea': 11421,
    'Valdés': 10958,
    'Luarca': 10958,  # Capital of Valdés
    'Carreño': 10177,
    'Luanco': 10177,  # Capital of Carreño
    'Gozón': 10470,
    'Tineo': 8769,
    'Grado': 9616,
    'Navia': 8136,
    'Pravia': 7830,
    'Piloña': 6822,
    'Cangas de Onís': 6316,
    'Ribadesella': 5548,
    'Parres': 5208,
    'Noreña': 5058,
    'Salas': 4806,
    'Cudillero': 4928,
    'Soto del Barco': 3807,
    'Vegadeo': 3885,
    'Tapia de Casariego': 3652,
    'Cabrales': 1918,
    'Boal': 1412,
    'El Entrego': 37978,  # Part of San Martín del Rey Aurelio / Langreo area
    'San Martín del Rey Aurelio': 14000,
    'Sama de Langreo': 37978,  # Part of Langreo
    'La Felguera': 37978,  # Part of Langreo
    'Arriondas': 5208,  # Capital of Parres
    'Moreda': 12324,  # Part of Aller
    'Moreda de Aller': 12324,
    'Sotrondio': 14000,  # Part of San Martín del Rey Aurelio
}

# SESPA 8 Areas Sanitarias (current structure)
AREAS_SANITARIAS = {
    'Área I - Jarrio': {
        'sede': 'Jarrio (Coaña)',
        'poblacion': 42600,
        'municipios': ['Valdés', 'Navia', 'Coaña', 'Villayón', 'El Franco',
                       'Tapia de Casariego', 'Castropol', 'Vegadeo', 'Luarca'],
    },
    'Área II - Cangas del Narcea': {
        'sede': 'Cangas del Narcea',
        'poblacion': 55000,
        'municipios': ['Tineo', 'Cangas del Narcea', 'Allande', 'Salas',
                       'Belmonte de Miranda', 'Somiedo', 'Teverga', 'Grado'],
    },
    'Área III - Avilés': {
        'sede': 'Avilés',
        'poblacion': 155000,
        'municipios': ['Avilés', 'Corvera', 'Gozón', 'Castrillón', 'Cudillero',
                       'Illas', 'Pravia', 'Soto del Barco', 'Piedras Blancas', 'Luanco'],
    },
    'Área IV - Oviedo': {
        'sede': 'Oviedo',
        'poblacion': 340000,
        'municipios': ['Oviedo', 'Noreña', 'Siero', 'Pola de Siero', 'Llanera',
                       'Lugones', 'Nava', 'Bimenes', 'Ribera de Arriba'],
    },
    'Área V - Gijón': {
        'sede': 'Gijón',
        'poblacion': 305000,
        'municipios': ['Gijón', 'Carreño', 'Villaviciosa', 'Piloña', 'Colunga', 'Caravia'],
    },
    'Área VI - Arriondas': {
        'sede': 'Arriondas',
        'poblacion': 35000,
        'municipios': ['Llanes', 'Ribadesella', 'Cangas de Onís', 'Cabrales',
                       'Parres', 'Arriondas'],
    },
    'Área VII - Mieres': {
        'sede': 'Mieres',
        'poblacion': 65000,
        'municipios': ['Mieres', 'Lena', 'Pola de Lena', 'Aller', 'Moreda', 'Moreda de Aller'],
    },
    'Área VIII - Langreo': {
        'sede': 'Langreo',
        'poblacion': 65000,
        'municipios': ['Langreo', 'San Martín del Rey Aurelio', 'Laviana',
                       'El Entrego', 'Sama de Langreo', 'La Felguera', 'Sotrondio'],
    },
}

# Reverse map: municipio -> area sanitaria
MUNICIPIO_TO_AREA = {}
for area, info in AREAS_SANITARIAS.items():
    for muni in info['municipios']:
        MUNICIPIO_TO_AREA[muni] = area

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Add columns
    for col in ['poblacion_municipio', 'area_sanitaria', 'poblacion_area']:
        try:
            cur.execute(f'ALTER TABLE cuadro_medico ADD COLUMN {col} TEXT')
        except sqlite3.OperationalError:
            pass

    # Update population
    updated = 0
    for muni, pop in POPULATION.items():
        cur.execute('UPDATE cuadro_medico SET poblacion_municipio = ? WHERE municipio = ?', (pop, muni))
        updated += cur.rowcount

    # Update area sanitaria
    for muni, area in MUNICIPIO_TO_AREA.items():
        pop_area = AREAS_SANITARIAS[area]['poblacion']
        cur.execute('UPDATE cuadro_medico SET area_sanitaria = ?, poblacion_area = ? WHERE municipio = ?',
                    (area, pop_area, muni))

    conn.commit()

    # Report
    cur.execute('SELECT COUNT(*) FROM cuadro_medico WHERE poblacion_municipio IS NOT NULL')
    with_pop = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM cuadro_medico')
    total = cur.fetchone()[0]
    print(f'Registros con poblacion: {with_pop}/{total}')

    cur.execute('SELECT COUNT(*) FROM cuadro_medico WHERE area_sanitaria IS NOT NULL')
    with_area = cur.fetchone()[0]
    print(f'Registros con area sanitaria: {with_area}/{total}')

    # Municipios sin dato de poblacion
    cur.execute("SELECT DISTINCT municipio FROM cuadro_medico WHERE poblacion_municipio IS NULL AND municipio != ''")
    missing = [r[0] for r in cur.fetchall()]
    if missing:
        print(f'\nMunicipios sin dato de poblacion ({len(missing)}):')
        for m in missing:
            cur.execute('SELECT COUNT(*) FROM cuadro_medico WHERE municipio = ?', (m,))
            cnt = cur.fetchone()[0]
            print(f'  {m}: {cnt} registros')

    # Create population reference table
    cur.execute('DROP TABLE IF EXISTS poblacion_municipios')
    cur.execute('''CREATE TABLE poblacion_municipios (
        municipio TEXT PRIMARY KEY,
        poblacion INTEGER,
        area_sanitaria TEXT,
        poblacion_area INTEGER
    )''')
    for muni, pop in POPULATION.items():
        area = MUNICIPIO_TO_AREA.get(muni, '')
        pop_area = AREAS_SANITARIAS.get(area, {}).get('poblacion', 0) if area else 0
        cur.execute('INSERT OR REPLACE INTO poblacion_municipios VALUES (?, ?, ?, ?)',
                    (muni, pop, area, pop_area))

    # Create areas sanitarias table
    cur.execute('DROP TABLE IF EXISTS areas_sanitarias')
    cur.execute('''CREATE TABLE areas_sanitarias (
        area TEXT PRIMARY KEY,
        sede TEXT,
        poblacion INTEGER
    )''')
    for area, info in AREAS_SANITARIAS.items():
        cur.execute('INSERT INTO areas_sanitarias VALUES (?, ?, ?)',
                    (area, info['sede'], info['poblacion']))

    conn.commit()

    # Summary by area sanitaria
    print('\nResumen por Area Sanitaria:')
    cur.execute('''
        SELECT area_sanitaria,
               COUNT(DISTINCT CASE WHEN aseguradora = 'ASISA' THEN profesional END) as asisa_prof,
               COUNT(DISTINCT CASE WHEN aseguradora != 'ASISA' THEN profesional END) as comp_prof,
               COUNT(DISTINCT municipio) as municipios,
               MAX(poblacion_area) as poblacion
        FROM cuadro_medico
        WHERE area_sanitaria IS NOT NULL
        GROUP BY area_sanitaria
        ORDER BY poblacion DESC
    ''')
    for row in cur.fetchall():
        print(f'  {row[0]}: ASISA={row[1]} prof, Comp={row[2]} prof, {row[3]} munis, {row[4]} hab.')

    conn.close()
    print('\nDatos de poblacion y areas sanitarias incorporados.')

if __name__ == '__main__':
    main()
