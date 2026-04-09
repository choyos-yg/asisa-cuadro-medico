"""
Consolida todos los CSVs raw en una base de datos SQLite.
Limpia datos básicos: encoding, filas vacías, centros sin profesional.
NO hace normalización de especialidades (eso es Fase 2).
"""
import csv
import sqlite3
import os
import re

BASE = os.path.join(os.path.dirname(__file__), '..')
DATA_DIR = os.path.join(BASE, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
DB_PATH = os.path.join(DATA_DIR, 'cuadro_medico.db')

CSV_FILES = [
    'asisa_raw.csv',
    'adeslas_raw.csv',
    'dkv_raw.csv',
    'mapfre_raw.csv',
    'sanitas_raw.csv',
]

COLUMNS = [
    'aseguradora',
    'especialidad_original',
    'profesional',
    'centro',
    'direccion',
    'telefono',
    'municipio',
    'zona',
    'pagina_pdf',
]

# Fix common encoding artifacts from PDF extraction
ENCODING_FIXES = {
    '\ufffd': '',  # Unicode replacement character
    '�': '',
    '\x00': '',
}

def fix_encoding(text):
    if not text:
        return ''
    for old, new in ENCODING_FIXES.items():
        text = text.replace(old, new)
    return text.strip()

def normalize_municipio(muni):
    """Basic municipio normalization (capitalization, known variants)."""
    if not muni:
        return ''
    muni = fix_encoding(muni).strip()

    # Known variants
    variants = {
        'OVIEDO CAPITAL': 'Oviedo',
        'OVIEDO': 'Oviedo',
        'GIJÓN': 'Gijón',
        'GIJON': 'Gijón',
        'Gijon': 'Gijón',
        'AVILÉS': 'Avilés',
        'AVILES': 'Avilés',
        'Aviles': 'Avilés',
        'LANGREO': 'Langreo',
        'MIERES': 'Mieres',
        'LLANES': 'Llanes',
        'CANGAS DE ONÍS': 'Cangas de Onís',
        'Cangas de Onis': 'Cangas de Onís',
        'POLA DE SIERO': 'Pola de Siero',
        'VILLAVICIOSA': 'Villaviciosa',
        'LUARCA': 'Luarca',
        'NAVIA': 'Navia',
        'GRADO': 'Grado',
        'PRAVIA': 'Pravia',
        'RIBADESELLA': 'Ribadesella',
        'PIEDRAS BLANCAS': 'Piedras Blancas',
        'LA FELGUERA': 'La Felguera',
        'SAMA DE LANGREO': 'Sama de Langreo',
        'NOREÑA': 'Noreña',
        'CANGAS DEL NARCEA': 'Cangas del Narcea',
        'VEGADEO': 'Vegadeo',
        'LENA': 'Lena',
        'Pola de Lena': 'Pola de Lena',
    }

    if muni in variants:
        return variants[muni]
    if muni.upper() in variants:
        return variants[muni.upper()]

    # Title case as default
    return muni.title() if muni.isupper() else muni

def is_valid_record(row):
    """Check if a row has enough data to be useful."""
    prof = row.get('profesional', '').strip()
    spec = row.get('especialidad_original', '').strip()

    # Must have at least a specialty
    if not spec:
        return False

    # Skip rows that are clearly not medical data
    skip_patterns = [
        'CONSULTA PREVIA', 'PREVIA PETICIÓN', 'PREVIA PETICION',
        'AMBULANCIAS', 'URGENCIAS HOSPITALARIAS',
        'RED NACIONAL', 'ÍNDICE', 'NORMAS DE UTILIZACIÓN',
        'CERTIFICACIÓN', 'ASISTENCIA EN VIAJE',
        'OFICINAS', 'AUTORIZACIONES',
    ]
    spec_upper = spec.upper()
    for pattern in skip_patterns:
        if pattern in spec_upper:
            return False

    return True

def load_csv(filepath):
    """Load and clean a raw CSV file."""
    records = []
    if not os.path.exists(filepath):
        print(f'  SKIP: {filepath} no existe')
        return records

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Clean all fields
            cleaned = {}
            for col in COLUMNS:
                val = row.get(col, '')
                cleaned[col] = fix_encoding(val) if val else ''

            # Normalize municipio
            cleaned['municipio'] = normalize_municipio(cleaned['municipio'])
            cleaned['zona'] = normalize_municipio(cleaned['zona'])

            # If zona is empty, use municipio
            if not cleaned['zona'] and cleaned['municipio']:
                cleaned['zona'] = cleaned['municipio']

            if is_valid_record(cleaned):
                records.append(cleaned)

    return records

def create_db(all_records):
    """Create SQLite database with all records."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Main table
    cur.execute('''
        CREATE TABLE cuadro_medico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            aseguradora TEXT NOT NULL,
            especialidad_original TEXT,
            profesional TEXT,
            centro TEXT,
            direccion TEXT,
            telefono TEXT,
            municipio TEXT,
            zona TEXT,
            pagina_pdf INTEGER
        )
    ''')

    # Insert records
    for r in all_records:
        cur.execute('''
            INSERT INTO cuadro_medico
            (aseguradora, especialidad_original, profesional, centro, direccion, telefono, municipio, zona, pagina_pdf)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            r['aseguradora'],
            r['especialidad_original'],
            r['profesional'],
            r['centro'],
            r['direccion'],
            r['telefono'],
            r['municipio'],
            r['zona'],
            int(r['pagina_pdf']) if r['pagina_pdf'] and r['pagina_pdf'].isdigit() else None,
        ))

    # Create indexes for common queries
    cur.execute('CREATE INDEX idx_aseguradora ON cuadro_medico(aseguradora)')
    cur.execute('CREATE INDEX idx_especialidad ON cuadro_medico(especialidad_original)')
    cur.execute('CREATE INDEX idx_municipio ON cuadro_medico(municipio)')
    cur.execute('CREATE INDEX idx_zona ON cuadro_medico(zona)')
    cur.execute('CREATE INDEX idx_profesional ON cuadro_medico(profesional)')

    conn.commit()
    return conn

def print_summary(conn):
    """Print database summary stats."""
    cur = conn.cursor()

    print('\n' + '='*60)
    print('RESUMEN BASE DE DATOS')
    print('='*60)

    cur.execute('SELECT COUNT(*) FROM cuadro_medico')
    print(f'\nTotal registros: {cur.fetchone()[0]}')

    print('\nPor aseguradora:')
    cur.execute('SELECT aseguradora, COUNT(*) as n FROM cuadro_medico GROUP BY aseguradora ORDER BY n DESC')
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]}')

    print('\nPor municipio (top 15):')
    cur.execute('SELECT municipio, COUNT(*) as n FROM cuadro_medico WHERE municipio != "" GROUP BY municipio ORDER BY n DESC LIMIT 15')
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]}')

    print('\nEspecialidades únicas por aseguradora:')
    cur.execute('SELECT aseguradora, COUNT(DISTINCT especialidad_original) as n FROM cuadro_medico GROUP BY aseguradora ORDER BY n DESC')
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]}')

    print('\nTop 20 especialidades (todas las aseguradoras):')
    cur.execute('SELECT especialidad_original, COUNT(*) as n FROM cuadro_medico GROUP BY especialidad_original ORDER BY n DESC LIMIT 20')
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]}')

    print('\nProfesionales con nombre (no vacío):')
    cur.execute("SELECT aseguradora, COUNT(*) FROM cuadro_medico WHERE profesional != '' GROUP BY aseguradora")
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]}')

def main():
    all_records = []

    for fname in CSV_FILES:
        path = os.path.join(RAW_DIR, fname)
        print(f'Cargando {fname}...')
        records = load_csv(path)
        print(f'  {len(records)} registros válidos')
        all_records.extend(records)

    print(f'\nTotal registros a insertar: {len(all_records)}')

    conn = create_db(all_records)
    print_summary(conn)

    conn.close()
    print(f'\nBase de datos creada: {DB_PATH}')
    print(f'Tamaño: {os.path.getsize(DB_PATH):,} bytes')

if __name__ == '__main__':
    main()
