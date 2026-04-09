"""
Extractor de cuadro médico Adeslas Asturias.
Estructura del PDF: organizado por ciudad/municipio, luego por especialidad, luego profesionales.
Páginas de contenido médico: ~6-100 (tras índice y urgencias).
"""
import pdfplumber
import csv
import re
import os

PDF_PATH = os.path.join(os.path.dirname(__file__), '..', 'Cuadro medico Adeslas Asturias.pdf')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'adeslas_raw.csv')

# Known specialties from the index (page 8 onwards)
SPECIALTIES = [
    'ALERGOLOGÍA', 'AMBULANCIAS', 'ANÁLISIS CLÍNICOS', 'ANATOMÍA PATOLÓGICA',
    'ANESTESIOLOGÍA Y REANIMACIÓN', 'ANGIOLOGÍA Y CIRUGÍA VASCULAR',
    'APARATO DIGESTIVO', 'AUDIOLOGÍA', 'CARDIOLOGÍA',
    'CIRUGÍA CARDIOVASCULAR', 'CIRUGÍA GENERAL Y DEL AP. DIGESTIVO',
    'CIRUGÍA GENERAL Y DEL APARATO DIGESTIVO',
    'CIRUGÍA ORAL Y MAXILOFACIAL', 'CIRUGÍA PEDIÁTRICA',
    'CIRUGÍA PLÁSTICA Y REPARADORA', 'CIRUGÍA TORÁCICA',
    'CIRUGÍA VASCULAR', 'DERMATOLOGÍA', 'DERMATOLOGÍA Y VENEREOLOGÍA',
    'DIETÉTICA Y NUTRICIÓN', 'ENDOCRINOLOGÍA', 'ENDOCRINOLOGÍA Y NUTRICIÓN',
    'ENFERMERÍA', 'ESTOMATOLOGÍA', 'ESTOMATOLOGÍA (ODONTOLOGÍA)',
    'FISIOTERAPIA', 'GINECOLOGÍA', 'GINECOLOGÍA Y OBSTETRICIA',
    'HEMATOLOGÍA', 'HEMATOLOGÍA Y HEMOTERAPIA',
    'LOGOPEDIA', 'MATRONAS', 'MEDICINA GENERAL', 'MEDICINA INTERNA',
    'MEDICINA NUCLEAR', 'NEFROLOGÍA', 'NEUMOLOGÍA', 'NEUROCIRUGÍA',
    'NEUROLOGÍA', 'NEUROFISIOLOGÍA', 'NEUROFISIOLOGÍA CLÍNICA',
    'OBSTETRICIA', 'ODONTOLOGÍA', 'OFTALMOLOGÍA',
    'ONCOLOGÍA', 'ONCOLOGÍA MÉDICA', 'ONCOLOGÍA RADIOTERÁPICA',
    'OTORRINOLARINGOLOGÍA', 'PEDIATRÍA', 'PODOLOGÍA',
    'PREPARACIÓN AL PARTO', 'PSICOLOGÍA', 'PSIQUIATRÍA',
    'RADIODIAGNÓSTICO', 'RADIOLOGÍA', 'REHABILITACIÓN',
    'REUMATOLOGÍA', 'TRAUMATOLOGÍA', 'TRAUMATOLOGÍA Y CIRUGÍA ORTOPÉDICA',
    'UROLOGÍA',
    # Additional from municipios
    'ATENCIÓN PRIMARIA', 'ATENCIÓN ESPECIALIZADA',
    'PERSONAL SANITARIO', 'MEDIOS COMPLEMENTARIOS DE DIAGNÓSTICO',
]

# Cities/zones in the PDF
MAIN_ZONES = {
    'OVIEDO': 'Oviedo',
    'GIJÓN': 'Gijón',
    'GIJON': 'Gijón',
}

def normalize_text(text):
    """Fix encoding issues from PDF extraction."""
    replacements = {
        '�': '',  # Remove replacement chars
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text.strip()

def is_specialty_header(line):
    """Check if a line is a specialty name."""
    clean = line.strip().upper()
    # Remove common prefixes
    clean = re.sub(r'^\d+\s*', '', clean)
    for spec in SPECIALTIES:
        if clean == spec or clean.startswith(spec):
            return spec
    # Check for specialty-like patterns (ALL CAPS, no numbers except in known patterns)
    if re.match(r'^[A-ZÁÉÍÓÚÑÜ\s\.\,\(\)Y/]+$', clean) and len(clean) > 3 and clean not in ('OVIEDO', 'GIJÓN', 'AVILES', 'AVILÉS'):
        return clean
    return None

def is_person_name(line):
    """Check if line looks like a person name (SURNAME, Name format)."""
    clean = line.strip()
    # Pattern: SURNAME SURNAME, Name or SURNAME, Name
    if re.match(r'^[A-ZÁÉÍÓÚÑÜ][A-ZÁÉÍÓÚÑÜ\s\-]+,\s+[A-Za-záéíóúñü]', clean):
        return True
    return False

def is_center_name(line):
    """Check if line looks like a medical center."""
    clean = line.strip().upper()
    center_keywords = ['HOSPITAL', 'CLÍNICA', 'CLINICA', 'CENTRO', 'POLICLÍNICA', 'POLICLINICA',
                       'SANATORIO', 'LABORATORIO', 'INSTITUTO', 'CONSULTORIO']
    return any(kw in clean for kw in center_keywords)

def extract_phone(text):
    """Extract phone numbers from text."""
    phones = re.findall(r'(?:Tel[eé]fono[s]?\s*)?(\d[\d\s\-]{7,})', text)
    return '; '.join(p.strip() for p in phones) if phones else ''

def detect_municipio(text, current_municipio):
    """Detect if we're entering a new municipio section."""
    # The PDF has municipality names as section headers
    municipalities = [
        'ALLER', 'AVILÉS', 'AVILES', 'BELMONTE DE MIRANDA', 'BOAL',
        'CABRALES', 'CANGAS DEL NARCEA', 'CANGAS DE ONÍS', 'CARAVIA',
        'CARREÑO', 'CASTRILLÓN', 'CASTROPOL', 'COLUNGA', 'CORVERA',
        'CUDILLERO', 'EL FRANCO', 'GOZÓN', 'GRADO', 'GRANDAS DE SALIME',
        'IBIAS', 'LANGREO', 'LAVIANA', 'LENA', 'LLANES', 'LUARCA',
        'MIERES', 'MIERES DEL CAMINO', 'NAVA', 'NAVIA', 'NOREÑA',
        'ONÍS', 'OVIEDO', 'PARRES', 'PILOÑA', 'PRAVIA', 'RIBADEDEVA',
        'RIBADESELLA', 'SALAS', 'SAN MARTÍN DEL REY AURELIO',
        'SIERO', 'TAPIA DE CASARIEGO', 'TINEO', 'VALDÉS',
        'VEGADEO', 'VILLAVICIOSA', 'MOREDA', 'GIJÓN',
        'LA FELGUERA', 'SAMA', 'POLA DE SIERO', 'POSADA DE LLANES',
    ]
    clean = text.strip().upper()
    for m in municipalities:
        if clean == m:
            return m.title()
    return current_municipio

def parse_adeslas():
    records = []

    with pdfplumber.open(PDF_PATH) as pdf:
        current_zona = ''
        current_municipio = ''
        current_specialty = ''
        current_center = ''
        current_person = ''
        current_address = ''
        current_phone = ''

        # Track which section we're in
        in_oviedo = False
        in_gijon = False
        in_municipios = False

        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue

            text = normalize_text(text)
            lines = text.split('\n')

            for line in lines:
                line = line.strip()
                if not line or len(line) < 2:
                    continue

                # Skip page numbers and headers
                if re.match(r'^\d+$', line):
                    continue
                if 'ndice' in line.lower() or 'normas de' in line.lower():
                    continue
                if line.startswith('% ') or line.startswith('www.'):
                    continue

                # Detect main zone changes
                if 'Cuadro Mdico de Oviedo' in line or 'Cuadro Médico de Oviedo' in line or line.strip() == 'Oviedo':
                    current_zona = 'Oviedo'
                    current_municipio = 'Oviedo'
                    in_oviedo = True
                    in_gijon = False
                    in_municipios = False
                    continue
                if 'Cuadro Mdico de Gijn' in line or 'Cuadro Médico de Gijón' in line or line.strip() == 'Gijón' or line.strip() == 'Gijn':
                    current_zona = 'Gijón'
                    current_municipio = 'Gijón'
                    in_oviedo = False
                    in_gijon = True
                    in_municipios = False
                    continue
                if 'Cuadro Mdico Municipios' in line or 'Cuadro Médico Municipios' in line:
                    in_oviedo = False
                    in_gijon = False
                    in_municipios = True
                    continue

                # Detect municipio in municipios section
                if in_municipios:
                    new_muni = detect_municipio(line, current_municipio)
                    if new_muni != current_municipio:
                        current_municipio = new_muni
                        current_zona = new_muni
                        continue

                # Detect specialty
                spec = is_specialty_header(line)
                if spec:
                    current_specialty = spec
                    continue

                # Detect center
                if is_center_name(line):
                    current_center = line.strip()
                    continue

                # Detect person
                if is_person_name(line):
                    # Save previous person if exists
                    if current_person and current_specialty:
                        records.append({
                            'aseguradora': 'Adeslas',
                            'especialidad_original': current_specialty,
                            'profesional': current_person,
                            'centro': current_center,
                            'direccion': current_address,
                            'telefono': current_phone,
                            'municipio': current_municipio,
                            'zona': current_zona,
                            'pagina_pdf': page_num + 1,
                        })
                    current_person = line.strip()
                    current_address = ''
                    current_phone = ''
                    continue

                # Detect address (starts with C/, Avda., Plaza, etc. or has postal code)
                if re.match(r'^(?:C/|Avda\.|Plaza|Calle|Paseo|Ctra\.|Prao|Naranjo|Quintana|Cervantes|Comandante|General|Marqu)', line):
                    current_address = line.strip()
                    continue

                # Detect phone
                phone = extract_phone(line)
                if phone and 'Telfono' in line or 'Teléfono' in line or re.match(r'.*Tel[eé]fono.*', line):
                    current_phone = phone
                    continue

            # Flush last person on page
            if current_person and current_specialty:
                records.append({
                    'aseguradora': 'Adeslas',
                    'especialidad_original': current_specialty,
                    'profesional': current_person,
                    'centro': current_center,
                    'direccion': current_address,
                    'telefono': current_phone,
                    'municipio': current_municipio,
                    'zona': current_zona,
                    'pagina_pdf': page_num + 1,
                })
                # Don't reset person - might continue on next page

    return records

def deduplicate(records):
    """Remove duplicate entries."""
    seen = set()
    unique = []
    for r in records:
        key = (r['profesional'], r['especialidad_original'], r['municipio'])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique

def main():
    print(f'Extrayendo Adeslas desde: {PDF_PATH}')
    records = parse_adeslas()
    records = deduplicate(records)

    # Write CSV
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    fieldnames = ['aseguradora', 'especialidad_original', 'profesional', 'centro',
                  'direccion', 'telefono', 'municipio', 'zona', 'pagina_pdf']

    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f'Extraídos {len(records)} registros')
    print(f'Guardado en: {OUTPUT_PATH}')

    # Summary
    specialties = set(r['especialidad_original'] for r in records)
    municipios = set(r['municipio'] for r in records if r['municipio'])
    print(f'Especialidades encontradas: {len(specialties)}')
    print(f'Municipios: {len(municipios)}')
    for s in sorted(specialties):
        count = sum(1 for r in records if r['especialidad_original'] == s)
        print(f'  {s}: {count}')

if __name__ == '__main__':
    main()
