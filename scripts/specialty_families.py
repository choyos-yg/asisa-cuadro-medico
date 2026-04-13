"""
Agrupa especialidades afines en familias.
Esto permite matchear correctamente a un profesional entre aseguradoras
incluso cuando aparece con especialidades distintas pero relacionadas.

Ejemplo: un odontólogo puede aparecer como "Odontología" en una y
"Odontología - Ortodoncia" en otra. Es el mismo médico.

Pero NO agrupamos cosas no relacionadas (Angiología ≠ Odontología).
"""
import sqlite3
import os

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')

# Grupos de especialidades afines.
# Clave: nombre de la familia.
# Valor: lista de especialidades que se consideran parte de la misma familia.
FAMILIES = {
    'Odontología/Maxilofacial': [
        'Odontología',
        'Odontología - Ortodoncia',
        'Odontología - Periodoncia',
        'Cirugía Oral y Maxilofacial',
    ],
    'Cardiología': [
        'Cardiología',
        'Pruebas Diagnósticas Cardiológicas',
        'Cirugía Cardiovascular',
    ],
    'Radiología/Diagnóstico por Imagen': [
        'Radiodiagnóstico',
        'Ecografía',
        'Mamografía',
        'TAC',
        'Resonancia Magnética',
        'Densitometría Ósea',
        'Diagnóstico Prenatal',
    ],
    'Digestivo': [
        'Aparato Digestivo',
        'Endoscopia Digestiva',
    ],
    'Ginecología/Obstetricia': [
        'Ginecología y Obstetricia',
        'Matronas',
    ],
    'Salud Mental': [
        'Psicología',
        'Psiquiatría',
    ],
    'Rehabilitación': [
        'Fisioterapia',
        'Logopedia',
    ],
    'Oncología': [
        'Oncología',
        'Oncología Médica',
        'Oncología Radioterápica',
    ],
    'Hematología': [
        'Hematología',
    ],
    'Traumatología/Reumatología': [
        'Traumatología',
        'Reumatología',
    ],
}

# Reverse map: especialidad -> familia
SPECIALTY_TO_FAMILY = {}
for family, specs in FAMILIES.items():
    for spec in specs:
        SPECIALTY_TO_FAMILY[spec] = family


def get_family(specialty):
    """Devuelve la familia de una especialidad, o la especialidad misma si no tiene familia definida."""
    if not specialty:
        return ''
    return SPECIALTY_TO_FAMILY.get(specialty, specialty)


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Add column
    try:
        cur.execute('ALTER TABLE cuadro_medico ADD COLUMN familia_especialidad TEXT')
    except sqlite3.OperationalError:
        pass

    # Update all records
    cur.execute('SELECT DISTINCT especialidad_normalizada FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL')
    specs = [r[0] for r in cur.fetchall()]

    for spec in specs:
        family = get_family(spec)
        cur.execute('UPDATE cuadro_medico SET familia_especialidad = ? WHERE especialidad_normalizada = ?',
                    (family, spec))

    # Create index
    cur.execute('CREATE INDEX IF NOT EXISTS idx_familia ON cuadro_medico(familia_especialidad)')

    conn.commit()

    # Report
    print('=== FAMILIAS DE ESPECIALIDADES ===\n')
    cur.execute('''
        SELECT familia_especialidad, COUNT(DISTINCT especialidad_normalizada) as n_esp,
               GROUP_CONCAT(DISTINCT especialidad_normalizada) as especialidades,
               COUNT(*) as registros
        FROM cuadro_medico
        WHERE familia_especialidad IS NOT NULL
        GROUP BY familia_especialidad
        ORDER BY registros DESC
    ''')
    for r in cur.fetchall():
        n_esp = r[1]
        if n_esp > 1:
            print(f'[{r[3]:4d} registros] {r[0]}')
            print(f'                 incluye: {r[2]}')
            print()

    # Validation: ahora cuántos profesionales con 3+ familias distintas (reales casos raros)?
    print('\n=== PROFESIONALES CON 3+ FAMILIAS DIFERENTES (casos sospechosos) ===\n')
    cur.execute('''
        SELECT profesional_norm, COUNT(DISTINCT familia_especialidad) as n_fam,
               GROUP_CONCAT(DISTINCT familia_especialidad) as familias,
               COUNT(DISTINCT aseguradora) as n_aseguradoras
        FROM cuadro_medico
        WHERE profesional_norm != '' AND familia_especialidad IS NOT NULL
        GROUP BY profesional_norm
        HAVING n_fam >= 3
        ORDER BY n_fam DESC
    ''')
    rows = cur.fetchall()
    print(f'Total casos: {len(rows)}')
    print(f'Estos son probablemente homónimos (personas distintas con mismo nombre) o errores de extraccion\n')
    for r in rows[:15]:
        print(f'  [{r[1]} familias, {r[3]} aseg.] {r[0]}')
        print(f'    {r[2]}')

    conn.close()
    print('\nFamilias de especialidades añadidas.')


if __name__ == '__main__':
    main()
