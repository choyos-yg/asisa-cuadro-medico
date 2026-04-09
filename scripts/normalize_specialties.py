"""
Normalización de especialidades médicas.
Aplica el mapping al SQLite y crea la columna especialidad_normalizada.
Decisiones del cliente ASISA incorporadas.
"""
import sqlite3
import csv
import os

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')
MAPPING_PATH = os.path.join(BASE, 'data', 'specialty_mapping.csv')

# === MASTER MAPPING ===
# Key: variante original (case-insensitive match)
# Value: especialidad normalizada
# Decisiones del cliente marcadas con [CLIENTE]

SPECIALTY_MAP = {
    # --- Alergología ---
    'ALERGOLOGIA': 'Alergología',
    'ALERGOLOGÍA': 'Alergología',
    'Alergologia': 'Alergología',

    # --- Análisis Clínicos ---
    'ANALISIS CLINICOS': 'Análisis Clínicos',
    'ANÁLISIS CLÍNICOS': 'Análisis Clínicos',
    'Analisis Clinicos': 'Análisis Clínicos',

    # --- Anatomía Patológica ---
    'ANATOMIA PATOLOGICA': 'Anatomía Patológica',
    'ANATOMÍA PATOLÓGICA': 'Anatomía Patológica',
    'Anatomia Patologica': 'Anatomía Patológica',

    # --- Anestesiología ---
    'ANESTESIOLOGIA Y REANIMACION': 'Anestesiología y Reanimación',
    'ANESTESIOLOGÍA Y REANIMACIÓN': 'Anestesiología y Reanimación',

    # --- Angiología y Cirugía Vascular ---
    'ANGIOLOGIA Y CIRUGIA VASCULAR': 'Angiología y Cirugía Vascular',
    'ANGIOLOGÍA Y CIRUGÍA VASCULAR': 'Angiología y Cirugía Vascular',
    'Angiologia y Cirugia Vascular': 'Angiología y Cirugía Vascular',

    # --- Aparato Digestivo ---
    'APARATO DIGESTIVO': 'Aparato Digestivo',
    'Aparato Digestivo': 'Aparato Digestivo',
    'DIGESTIVO': 'Aparato Digestivo',

    # --- Audiología → ORL [CLIENTE: son lo mismo] ---
    'AUDIOLOGÍA': 'Otorrinolaringología',

    # --- Cardiología ---
    'CARDIOLOGIA': 'Cardiología',
    'CARDIOLOGÍA': 'Cardiología',
    'Cardiologia': 'Cardiología',

    # --- Cirugía Cardiovascular ---
    'CIRUGIA CARDIOVASCULAR': 'Cirugía Cardiovascular',
    'CIRUGÍA CARDIOVASCULAR': 'Cirugía Cardiovascular',
    'Cirugia Cardiovascular': 'Cirugía Cardiovascular',

    # --- Cirugía General [CLIENTE: Cirugía General y del Ap. Digestivo = Cirugía General] ---
    'CIRUGIA GENERAL Y DEL APARATO DIGESTIVO': 'Cirugía General',
    'CIRUGIA GENERAL Y DIGESTIVO': 'Cirugía General',
    'Cirugia General y del Aparato Digestivo': 'Cirugía General',

    # --- Cirugía Oral y Maxilofacial ---
    'CIRUGIA ORAL O MAXILOFACIAL': 'Cirugía Oral y Maxilofacial',
    'CIRUGIA ORAL Y MAXILOFACIAL': 'Cirugía Oral y Maxilofacial',
    'CIRUGÍA ORAL Y MÁXILOFACIAL': 'Cirugía Oral y Maxilofacial',
    'CIRUGÍA MAXILOFACIAL': 'Cirugía Oral y Maxilofacial',
    'Cirugia Maxilofacial': 'Cirugía Oral y Maxilofacial',
    'CIRUGIA BUCAL': 'Cirugía Oral y Maxilofacial',

    # --- Cirugía Pediátrica ---
    'CIRUGIA PEDIATRICA': 'Cirugía Pediátrica',
    'CIRUGÍA PEDIÁTRICA': 'Cirugía Pediátrica',

    # --- Cirugía Plástica ---
    'CIRUGIA PLASTICA ESTETICA Y REPARADORA': 'Cirugía Plástica y Reparadora',
    'CIRUGIA PLASTICA Y REPARADORA': 'Cirugía Plástica y Reparadora',
    'CIRUGÍA PLÁSTICA Y REPARADORA': 'Cirugía Plástica y Reparadora',
    'Cirugia Plastica y Reparadora': 'Cirugía Plástica y Reparadora',

    # --- Cirugía Torácica ---
    'CIRUGIA TORACICA': 'Cirugía Torácica',
    'CIRUGÍA TORÁCICA': 'Cirugía Torácica',

    # --- Dermatología ---
    'DERMATOLOGIA': 'Dermatología',
    'DERMATOLOGIA MEDICO-QUIRURGICA Y VENEREO': 'Dermatología',
    'DERMATOLOGÍA Y VENEREOLOGÍA': 'Dermatología',
    'Dermatologia': 'Dermatología',

    # --- Diagnóstico por Imagen / Radiodiagnóstico ---
    'RADIODIAGNOSTICO': 'Radiodiagnóstico',
    'RADIODIAGNÓSTICO': 'Radiodiagnóstico',
    'RADIOLOGIA': 'Radiodiagnóstico',
    'RADIOLOGÍA CONVENCIONAL': 'Radiodiagnóstico',
    'RADIOLOGIA ASTURIANA': 'Radiodiagnóstico',
    'RADIOLOGIA MAXILOFACIAL': 'Radiodiagnóstico',
    'RADIOLOGÍA DENTAL': 'Radiodiagnóstico',
    'Diagnostico por la Imagen': 'Radiodiagnóstico',

    # --- Diagnóstico prenatal ---
    'DIAGNÓSTICO ESPECIAL PRENATAL': 'Diagnóstico Prenatal',

    # --- Densitometría ---
    'DENSITOMETRIA': 'Densitometría Ósea',
    'DENSITOMETRIA OSEA': 'Densitometría Ósea',
    'DENSITOMETRÍA ÓSEA': 'Densitometría Ósea',

    # --- Dietética y Nutrición ---
    'DIETETICA Y NUTRICION': 'Dietética y Nutrición',
    'DIETÉTICA Y NUTRICIÓN': 'Dietética y Nutrición',

    # --- Ecocardiografía (diagnóstico cardiológico) ---
    'ECOCARDIOGRAFÍA': 'Pruebas Diagnósticas Cardiológicas',
    'ERGOMETRÍA': 'Pruebas Diagnósticas Cardiológicas',
    'HOLTER': 'Pruebas Diagnósticas Cardiológicas',
    'PRUEBAS DE DIAGNOSTICO CARDIOLOGICO NO INVASIVO': 'Pruebas Diagnósticas Cardiológicas',
    'ECO-DOPPLER': 'Pruebas Diagnósticas Cardiológicas',

    # --- Ecografía ---
    'ECOGRAFIA': 'Ecografía',
    'ECOGRAFIAS': 'Ecografía',
    'ECOGRAFÍA UROLÓGICA': 'Ecografía',

    # --- Endocrinología ---
    'ENDOCRINOLOGIA': 'Endocrinología y Nutrición',
    'ENDOCRINOLOGIA Y NUTRICION': 'Endocrinología y Nutrición',
    'ENDOCRINOLOGÍA Y NUTRICIÓN': 'Endocrinología y Nutrición',
    'ENDOCRINO Y NUTRICIÓN': 'Endocrinología y Nutrición',
    'Endocrinologia y Nutricion': 'Endocrinología y Nutrición',

    # --- Endoscopia ---
    'ENDOSCOPIA DIGESTIVA': 'Endoscopia Digestiva',
    'ENDOSCOPIAS': 'Endoscopia Digestiva',

    # --- Enfermería ---
    'ENFERMERIA': 'Enfermería',
    'ENFERMERÍA': 'Enfermería',
    'Enfermeria': 'Enfermería',

    # --- Fisioterapia y Rehabilitación ---
    'FISIOTERAPIA': 'Fisioterapia',
    'FISIOTERAPEUTAS Y CENTROS DE FISIOTERAPIA': 'Fisioterapia',
    'MEDICINA FISICA Y REHABILITACION': 'Fisioterapia',
    'REHABILITACIÓN': 'Fisioterapia',
    'REHABILITADORES Y CENTROS DE REHABILITACION': 'Fisioterapia',
    'Rehabilitacion': 'Fisioterapia',
    'Rehabilitacion Fisioterapeutas': 'Fisioterapia',
    'Rehabilitacion Medicos Rehabilitadores': 'Fisioterapia',

    # --- Genética ---
    'GENETICA HUMANA': 'Genética',

    # --- Geriatría ---
    'GERIATRIA': 'Geriatría',

    # --- Ginecología ---
    'GINECOLOGIA': 'Ginecología y Obstetricia',
    'GINECOLOGIA Y OBSTETRICIA': 'Ginecología y Obstetricia',
    'GINECOLOGÍA': 'Ginecología y Obstetricia',
    'GINECOLOGÍA Y OBSTETRICIA': 'Ginecología y Obstetricia',
    'Obstetricia y Ginecologia': 'Ginecología y Obstetricia',
    'OBSTETRICIA': 'Ginecología y Obstetricia',

    # --- Hematología ---
    'HEMATOLOGIA': 'Hematología',
    'HEMATOLOGIA Y HEMOTERAPIA': 'Hematología',
    'HEMATOLOGÍA Y HEMOTERAPIA': 'Hematología',
    'Hematologia Hemoterapia': 'Hematología',
    'Hematologia y Hemoterapia': 'Hematología',

    # --- Implantología (dental) ---
    'IMPLANTOLOGIA': 'Odontología',
    'ODONTOLOGÍA IMPLANTES': 'Odontología',

    # --- Logopedia [CLIENTE: Logopedia = Foniatría] ---
    'LOGOPEDIA': 'Logopedia',
    'LOGOPEDIA Y FONIATRIA': 'Logopedia',
    'LOGOFONATRIA': 'Logopedia',
    'Rehabilitacion del Lenguaje Logopedia': 'Logopedia',

    # --- Mamografía ---
    'MAMOGRAFIA': 'Mamografía',
    'MAMOGRAFÍA': 'Mamografía',

    # --- Matronas / Preparación al parto [CLIENTE: son lo mismo, independiente de ginecología] ---
    'MATRONA': 'Matronas',
    'MATRONAS': 'Matronas',
    'PREPARACION AL PARTO': 'Matronas',
    'PREPARACIÓN AL PARTO': 'Matronas',
    'Preparacion al Parto': 'Matronas',

    # --- Medicina General [CLIENTE: = Medicina de Familia] ---
    'MEDICINA GENERAL': 'Medicina General',
    'Medicina General': 'Medicina General',
    'MEDICINA FAMILIAR Y COMUNITARIA': 'Medicina General',

    # --- Medicina Interna ---
    'MEDICINA INTERNA': 'Medicina Interna',
    'Medicina Interna': 'Medicina Interna',

    # --- Medicina Nuclear ---
    'MEDICINA NUCLEAR': 'Medicina Nuclear',
    'MEDICINA NUCLEAR GEMINIS CLINICA': 'Medicina Nuclear',
    'Medicina Nuclear': 'Medicina Nuclear',

    # --- Microbiología ---
    'MICROBIOLOGÍA Y PARASITOLOGÍA': 'Microbiología',

    # --- Nefrología ---
    'NEFROLOGIA': 'Nefrología',
    'NEFROLOGÍA': 'Nefrología',
    'Nefrologia': 'Nefrología',

    # --- Neumología ---
    'NEUMOLOGIA': 'Neumología',
    'NEUMOLOGÍA': 'Neumología',
    'Neumologia': 'Neumología',

    # --- Neurocirugía ---
    'NEUROCIRUGIA': 'Neurocirugía',
    'NEUROCIRUGÍA': 'Neurocirugía',
    'Neurocirugia': 'Neurocirugía',

    # --- Neurofisiología ---
    'NEUROFISIOLOGIA CLINICA': 'Neurofisiología Clínica',
    'NEUROFISIOLOGÍA': 'Neurofisiología Clínica',
    'NEUROFISIOLOGÍA CLÍNICA': 'Neurofisiología Clínica',
    'Neurofisiologia Clinica': 'Neurofisiología Clínica',

    # --- Neurología ---
    'NEUROLOGIA': 'Neurología',
    'NEUROLOGÍA': 'Neurología',
    'Neurologia': 'Neurología',

    # --- Neuropsicología (independiente de Psicología) [CLIENTE] ---
    # Se mantiene separada — ver Psicología más abajo

    # --- Odontología [CLIENTE: Estomatología = Odontología] ---
    'ODONTOLOGIA - ESTOMATOLOGIA': 'Odontología',
    'ESTOMATOLOGÍA (ODONTOLOGÍA)': 'Odontología',
    'Odontoestomatologia (Dental)': 'Odontología',
    'ODONTOLOGÍA GENERAL': 'Odontología',
    'ODONTOLOGIA CONSERVADORA': 'Odontología',
    'ODONTOLOGIA PREVENTIVA': 'Odontología',
    'ESTOMATOLOGIA DE URGENCIA': 'Odontología',
    'ODONTOLOGÍA ORTODONCIA': 'Odontología - Ortodoncia',
    'ORTODONCIA': 'Odontología - Ortodoncia',
    'PERIODONCIA': 'Odontología - Periodoncia',

    # --- Oftalmología ---
    'OFTALMOLOGIA': 'Oftalmología',
    'OFTALMOLOGÍA': 'Oftalmología',
    'Oftalmologia': 'Oftalmología',

    # --- Oncología ---
    'ONCOLOGIA': 'Oncología',
    'ONCOLOGIA MEDICA': 'Oncología Médica',
    'ONCOLOGÍA MÉDICA': 'Oncología Médica',
    'Oncologia': 'Oncología',
    'Oncologia Medica': 'Oncología Médica',
    'ONCOLOGIA RADIOTERAPICA': 'Oncología Radioterápica',
    'RADIOTERAPIA': 'Oncología Radioterápica',

    # --- Osteopatía ---
    'Osteopatia': 'Osteopatía',

    # --- Otorrinolaringología [CLIENTE: incluye Audiología] ---
    'OTORRINOLARINGOLOGIA': 'Otorrinolaringología',
    'OTORRINOLARINGOLOGÍA': 'Otorrinolaringología',
    'Otorrinolaringologia': 'Otorrinolaringología',

    # --- Oxigenoterapia ---
    'Oxigenoterapia y Aerosoles': 'Oxigenoterapia',

    # --- Pediatría ---
    'PEDIATRIA': 'Pediatría',
    'PEDIATRÍA': 'Pediatría',
    'PEDIATRIA-MEDICO PUERICULTOR': 'Pediatría',
    'Pediatria y Puericultura': 'Pediatría',

    # --- Podología [CLIENTE: es especialidad] ---
    'PODOLOGIA': 'Podología',
    'PODOLOGÍA': 'Podología',
    'Podologia': 'Podología',
    'ESTUDIO BIOMECANICO DE LA MARCHA / PISADA SERVICIOS PODOACITVA': 'Podología',
    'ESTUDIO BIOMECÁNICO DE LA MARCHA': 'Podología',

    # --- Proctología ---
    'PROCTOLOGÍA': 'Proctología',

    # --- Psicología (NO incluye Neuropsicología) [CLIENTE] ---
    'PSICOLOGIA': 'Psicología',
    'PSICOLOGÍA': 'Psicología',
    'PSICOSABRE PSICOLOGOS': 'Psicología',
    'Tratamientos de Psicoterapia': 'Psicología',

    # --- Psiquiatría ---
    'PSIQUIATRIA': 'Psiquiatría',
    'PSIQUIATRÍA': 'Psiquiatría',
    'Psiquiatria': 'Psiquiatría',
    'Clinica Psiquiatrica': 'Psiquiatría',
    'Clinica Psiquiatrica Somio': 'Psiquiatría',
    'HOSPITAL PSIQUIATRICO': 'Psiquiatría',
    'HOSPITALIZACI\u00d3N PSIQUI\u00c1TRICA': 'Psiquiatría',

    # --- Resonancia Magnética ---
    'RESONANCIA MAGNETICA': 'Resonancia Magnética',
    'RESONANCIA MAGNÉTICA NUCLEAR. R.M.N.': 'Resonancia Magnética',
    'RESONANCIA NUCLEAR MAGNETICA': 'Resonancia Magnética',
    'RESONANCIA ABIERTA TORENO S.L.': 'Resonancia Magnética',
    'R.M.N.': 'Resonancia Magnética',

    # --- Reumatología ---
    'REUMATOLOGIA': 'Reumatología',
    'REUMATOLOGÍA': 'Reumatología',
    'Reumatologia': 'Reumatología',

    # --- TAC ---
    'T.A.C.': 'TAC',
    'TOMOGRAFIA COMPUTARIZADA': 'TAC',
    'TOMOGRAFÍA AXIAL COMPUTERIZADA. T.A.C.': 'TAC',

    # --- Tratamiento del dolor ---
    'TRATAMIENTO DEL DOLOR': 'Tratamiento del Dolor',

    # --- Traumatología ---
    'TRAUMATOLOGIA': 'Traumatología',
    'TRAUMATOLOGIA Y CIRUGIA ORTOPEDICA': 'Traumatología',
    'TRAUMATOLOGÍA Y CIRUGÍA ORTOPÉDICA': 'Traumatología',
    'TRAUMATOLOGÍA Y ORTOPEDIA': 'Traumatología',
    'Traumatologia y Ortopedia': 'Traumatología',
    'Traumatologia': 'Traumatología',

    # --- Urgencias (no son especialidad, pero las mantenemos para conteo de centros) ---
    'URGENCIAS GENERALES': 'Urgencias',
    'URGENCIAS AMBULATORIAS': 'Urgencias',
    'Urgencia Ambulatoria': 'Urgencias',
    'Urgencias Ambulatorias': 'Urgencias',

    # --- Urología ---
    'UROLOGIA': 'Urología',
    'UROLOGÍA': 'Urología',
    'Urologia': 'Urología',

    # --- Entries to EXCLUDE (not medical specialties) ---
    'CANGAS DEL NARCEA': None,
    'CENTROS HOSPITALARIOS CONCERTADOS': None,
    'CLINICAS CARDALIS': None,
    'CLINICAS Y HOSPITALES CONCERTADOS': None,
    'Clinica U Hospital': None,
    'GESTION DE CENTROS MEDICOS MAPFRE': None,
    'HOSPITALES Y CLINICAS': None,
    'HOSPITALIZACI\u00d3N GENERAL': None,
    'HOSPITAL DE D\u00cdA ONCOL\u00d3GICO': None,
    'LASERTERAPIA PROST\u00c1TICA': None,
    'Por Tu Salud Clinicas': None,
    'SOLO ATENCION DOMICILIARIA': None,
    'ASISTENCIA DE MEDICINA GENERAL EN': None,
    'TRATAMIENTOS ESPECIALES': None,
    'POSTPARTO': None,
    'DOMICILIO': None,
    'BIENESTAR': None,
    'CHEQUEO M\u00c9DICO GENERAL ANUAL DE': None,
    'OVIEDO CAPITAL': None,
    'MEDIOS COMPLEMENTARIOS DE': None,
}


def apply_normalization():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Add column if not exists
    try:
        cur.execute('ALTER TABLE cuadro_medico ADD COLUMN especialidad_normalizada TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Get all distinct specialties
    cur.execute('SELECT DISTINCT especialidad_original FROM cuadro_medico')
    all_specs = [row[0] for row in cur.fetchall()]

    mapped = 0
    unmapped = []
    excluded = 0

    for spec in all_specs:
        normalized = SPECIALTY_MAP.get(spec)
        if normalized is None and spec in SPECIALTY_MAP:
            # Explicitly excluded
            cur.execute('DELETE FROM cuadro_medico WHERE especialidad_original = ?', (spec,))
            excluded += cur.rowcount
        elif normalized:
            cur.execute('UPDATE cuadro_medico SET especialidad_normalizada = ? WHERE especialidad_original = ?',
                       (normalized, spec))
            mapped += cur.rowcount
        else:
            unmapped.append(spec)

    conn.commit()

    # Report
    print(f'Registros mapeados: {mapped}')
    print(f'Registros excluidos: {excluded}')
    print(f'Especialidades sin mapear: {len(unmapped)}')
    if unmapped:
        for spec in sorted(unmapped):
            cur.execute('SELECT COUNT(*) FROM cuadro_medico WHERE especialidad_original = ?', (spec,))
            count = cur.fetchone()[0]
            print(f'  [{count:3d}] {spec}')

    # Summary
    print(f'\n--- Resumen post-normalización ---')
    cur.execute('SELECT COUNT(*) FROM cuadro_medico')
    print(f'Total registros: {cur.fetchone()[0]}')

    cur.execute('SELECT COUNT(DISTINCT especialidad_normalizada) FROM cuadro_medico WHERE especialidad_normalizada IS NOT NULL')
    print(f'Especialidades normalizadas: {cur.fetchone()[0]}')

    print('\nTop 20 especialidades normalizadas:')
    cur.execute('''SELECT especialidad_normalizada, COUNT(*) as n
                   FROM cuadro_medico
                   WHERE especialidad_normalizada IS NOT NULL
                   GROUP BY especialidad_normalizada ORDER BY n DESC LIMIT 20''')
    for row in cur.fetchall():
        print(f'  {row[0]:40s} {row[1]:5d}')

    print('\nPor aseguradora:')
    cur.execute('''SELECT aseguradora, COUNT(*), COUNT(DISTINCT especialidad_normalizada)
                   FROM cuadro_medico
                   WHERE especialidad_normalizada IS NOT NULL
                   GROUP BY aseguradora ORDER BY COUNT(*) DESC''')
    for row in cur.fetchall():
        print(f'  {row[0]:10s}: {row[1]:5d} registros, {row[2]:3d} especialidades')

    # Export mapping CSV
    mapping_rows = []
    for original, normalized in sorted(SPECIALTY_MAP.items()):
        if normalized:
            mapping_rows.append({'original': original, 'normalizada': normalized})
    with open(MAPPING_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['original', 'normalizada'])
        writer.writeheader()
        writer.writerows(mapping_rows)
    print(f'\nMapping exportado: {MAPPING_PATH} ({len(mapping_rows)} entradas)')

    conn.close()

if __name__ == '__main__':
    apply_normalization()
