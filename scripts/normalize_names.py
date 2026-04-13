"""
Normaliza nombres de centros y profesionales para permitir matching cross-aseguradora.
Añade columnas `centro_norm` y `profesional_norm` al SQLite.
"""
import sqlite3
import re
import unicodedata
import os

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')

# Prefijos/tratamientos que se eliminan de nombres de profesionales
PROF_PREFIXES = [
    r'^dr\.?\s*', r'^dra\.?\s*', r'^sr\.?\s*', r'^sra\.?\s*',
    r'^d\.\s*', r'^d�\.\s*', r'^do�a\.?\s*', r'^don\.?\s*',
]

# Palabras "ruido" en centros que no aportan al matching
CENTER_NOISE = [
    r'\bs\.?\s*l\.?\s*$', r'\bs\.?\s*l\.?\s*u\.?\s*$',  # S.L., S.L.U.
    r'\bs\.?\s*a\.?\s*$', r'\bs\.?\s*a\.?\s*u\.?\s*$',  # S.A., S.A.U.
    r'^dr\.?\s+', r'^dra\.?\s+',  # Prefijo Dr./Dra. en centro
    r'\(policlinica\)', r'\(policl�nica\)',
]

def strip_accents(s):
    """Remueve acentos y caracteres especiales."""
    if not s:
        return ''
    # Replace common encoding artifacts
    s = s.replace('�', 'n')  # Often ñ corrupted
    # Normalize unicode
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))

def normalize_center(name):
    """Normaliza nombre de centro para matching."""
    if not name:
        return ''
    s = strip_accents(name).lower().strip()

    # Si el centro es una dirección (empieza por "calle", "avda", "c/", "plaza"), descartar
    addr_patterns = [r'^c/', r'^avda\.?\s', r'^avenida', r'^calle', r'^plaza', r'^ctra',
                     r'^paseo', r'^naranjo', r'^quintana', r'^comandante', r'^general',
                     r'^cervantes', r'^marqu', r'^prao', r'^\d']
    for p in addr_patterns:
        if re.match(p, s):
            # Only keep portion after "/" if any (some ASISA records have "ADDR / CENTER")
            if '/' in s:
                s = s.split('/')[-1].strip()
            else:
                return ''

    # Split on "/" and take the centro name (longest non-addr part)
    if '/' in s:
        parts = [p.strip() for p in s.split('/')]
        # Prefer parts that don't look like addresses
        non_addr = [p for p in parts if not any(re.match(pat, p) for pat in addr_patterns)]
        if non_addr:
            s = max(non_addr, key=len)

    # Remove noise patterns
    for pattern in CENTER_NOISE:
        s = re.sub(pattern, '', s, flags=re.IGNORECASE)

    # Remove quotes and all punctuation
    s = re.sub(r'[\'"""`]', '', s)
    s = re.sub(r'[,\.\-\(\)\[\]/]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()

    # Known aliases
    aliases = {
        'centro medico asturias': 'centro medico de asturias',
        'cl asturias': 'clinica asturias',
        'hospital begona': 'hospital begona',
        'hospital begon': 'hospital begona',
        'hospital begoha': 'hospital begona',  # Sanitas corruption
        'clinica rozona': 'policlinica rozona',  # Mapfre sin "Poli"
        'clinica rozana': 'policlinica rozona',  # Variante
        'policlinicas oviedo': 'policlinica oviedo',
        'policlinicas begona': 'policlinica begona',
        'policlinica rozana': 'policlinica rozona',  # Sanitas typo
        'policlinico sierosalud': 'policlinico sierosalud',
        'policlinico sierucalud': 'policlinico sierosalud',  # Sanitas typo
        'radiologo asturiana': 'radiologia asturiana',
        'dr centro medico de asturias': 'centro medico de asturias',
        'clinica asturias policlinica': 'clinica asturias',
        'centro medico mapfre salud gijon': 'centro medico mapfre gijon',
        'centro medico mapfre': 'centro medico mapfre',
    }
    return aliases.get(s, s)

def normalize_professional(name):
    """Normaliza nombre de profesional para matching.
    Formato estándar: 'apellido1 apellido2 nombre' (sin comas, sin prefijos, sin acentos, lowercase)
    """
    if not name:
        return ''

    # Si contiene múltiples profesionales separados por ';', tomar solo el primero
    if ';' in name:
        name = name.split(';')[0]

    s = strip_accents(name).lower().strip()

    # Remove prefixes (Dr., Dra., Sr., Sra., etc.)
    for p in PROF_PREFIXES:
        s = re.sub(p, '', s)

    # Has comma? Format: "apellidos, nombre" → "apellidos nombre"
    if ',' in s:
        parts = s.split(',', 1)
        s = f"{parts[0].strip()} {parts[1].strip()}"

    # Remove dots and extra punctuation
    s = re.sub(r'[\.]+', ' ', s)
    s = re.sub(r'[^\w\s-]', ' ', s)

    # Normalize whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Add columns
    for col in ['centro_norm', 'profesional_norm']:
        try:
            cur.execute(f'ALTER TABLE cuadro_medico ADD COLUMN {col} TEXT')
        except sqlite3.OperationalError:
            pass

    # Get all records and update normalization
    cur.execute('SELECT id, centro, profesional FROM cuadro_medico')
    rows = cur.fetchall()

    for id_, centro, prof in rows:
        cn = normalize_center(centro)
        pn = normalize_professional(prof)
        cur.execute('UPDATE cuadro_medico SET centro_norm = ?, profesional_norm = ? WHERE id = ?',
                    (cn, pn, id_))

    # Create index for matching
    cur.execute('CREATE INDEX IF NOT EXISTS idx_centro_norm ON cuadro_medico(centro_norm)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_profesional_norm ON cuadro_medico(profesional_norm)')

    conn.commit()

    # Verification
    print('=== VERIFICACION ===\n')

    print('1. Centros normalizados (ejemplos):')
    cur.execute('''
        SELECT centro_norm, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras, COUNT(*) as n
        FROM cuadro_medico
        WHERE centro_norm != ''
        GROUP BY centro_norm
        HAVING COUNT(DISTINCT aseguradora) >= 3
        ORDER BY COUNT(DISTINCT aseguradora) DESC, n DESC
        LIMIT 15
    ''')
    print(f'{"Centro normalizado":<50s} {"Aseguradoras":<30s} {"N":>4s}')
    for r in cur.fetchall():
        print(f'  {r[0]:<50s} {r[1]:<30s} {r[2]:>4d}')

    print('\n2. Profesionales compartidos (aparecen en 3+ aseguradoras):')
    cur.execute('''
        SELECT profesional_norm, GROUP_CONCAT(DISTINCT aseguradora) as aseguradoras, COUNT(*) as n
        FROM cuadro_medico
        WHERE profesional_norm != ''
        GROUP BY profesional_norm
        HAVING COUNT(DISTINCT aseguradora) >= 3
        ORDER BY COUNT(DISTINCT aseguradora) DESC, n DESC
        LIMIT 15
    ''')
    print(f'{"Profesional normalizado":<55s} {"Aseguradoras":<30s} {"N":>4s}')
    for r in cur.fetchall():
        print(f'  {r[0]:<55s} {r[1]:<30s} {r[2]:>4d}')

    print('\n3. Stats generales:')
    cur.execute("SELECT COUNT(*) FROM cuadro_medico WHERE centro_norm != ''")
    print(f'  Registros con centro normalizado: {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(*) FROM cuadro_medico WHERE profesional_norm != ''")
    print(f'  Registros con profesional normalizado: {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(DISTINCT centro_norm) FROM cuadro_medico WHERE centro_norm != ''")
    print(f'  Centros únicos (normalizados): {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(DISTINCT profesional_norm) FROM cuadro_medico WHERE profesional_norm != ''")
    print(f'  Profesionales únicos (normalizados): {cur.fetchone()[0]}')

    conn.close()
    print('\nNormalización completada.')

if __name__ == '__main__':
    main()
