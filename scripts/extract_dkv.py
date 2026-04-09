"""
Extractor de cuadro médico DKV Asturias.
Estructura del PDF: organizado por ciudad, luego por especialidad, luego centros y profesionales.
Header format: "> SPECIALTY | CITY <" or "> CITY | SPECIALTY <"
Medical content starts around page 24.
"""
import pdfplumber
import csv
import re
import os

PDF_PATH = os.path.join(os.path.dirname(__file__), '..', 'Cuadro medico DKV Asturias.pdf')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'dkv_raw.csv')

def normalize_text(text):
    replacements = {'�': ''}
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text.strip()

def extract_phone(text):
    phones = re.findall(r'Tel\.?:\s*([\d\s\-]+(?:\s*-\s*[\d\s\-]+)*)', text)
    return '; '.join(p.strip() for p in phones) if phones else ''

def is_person_name(line):
    clean = line.strip()
    # DKV format: "Surname Surname, Name" or "Surname - Surname Surname, Name"
    if re.match(r'^[A-ZÁÉÍÓÚÑÜa-záéíóúñü][a-záéíóúñüA-ZÁÉÍÓÚÑÜ\s\-]+,\s+[A-ZÁÉÍÓÚÑÜa-z]', clean):
        return True
    return False

def is_center_line(line):
    clean = line.strip()
    center_keywords = ['Hospital', 'Clínica', 'Clinica', 'Centro', 'Policlínica', 'Policlinica',
                       'Sanatorio', 'Laboratorio', 'Instituto', 'Consultorio', 'Merino Laboratorio',
                       'Maxilo', 'Rehavitall']
    return any(kw in clean for kw in center_keywords)

def parse_header(line):
    """Parse DKV section headers like '> SPECIALTY | CITY <' or '> CITY | SPECIALTY <'"""
    match = re.match(r'>\s*(.+?)\s*\|\s*(.+?)\s*<', line)
    if match:
        part1 = match.group(1).strip()
        part2 = match.group(2).strip()

        # Known cities
        cities = ['OVIEDO CAPITAL', 'OVIEDO', 'GIJÓN', 'GIJON', 'AVILÉS', 'AVILES',
                  'CANGAS DE ONÍS', 'CIAÑO DE LANGREO', 'GRADO', 'LA FELGUERA',
                  'LLANES', 'LUARCA', 'MIERES', 'PIEDRAS BLANCAS', 'POLA DE SIERO',
                  'SAMA DE LANGREO', 'VILLAVICIOSA', 'NAVIA', 'PRAVIA', 'NOREÑA',
                  'CANGAS DEL NARCEA', 'ASTURIAS PROVINCIA']

        if part1.upper() in cities:
            return part1, part2
        elif part2.upper() in cities:
            return part2, part1
        else:
            # Guess: shorter one is probably city
            return part2, part1
    return None, None

def parse_dkv():
    records = []

    with pdfplumber.open(PDF_PATH) as pdf:
        current_city = ''
        current_specialty = ''
        current_center = ''
        current_person = ''
        current_address = ''
        current_phone = ''

        # Content starts around page 24
        for page_num, page in enumerate(pdf.pages):
            if page_num < 23:  # Skip intro pages
                continue

            text = page.extract_text()
            if not text:
                continue

            text = normalize_text(text)
            lines = text.split('\n')

            i = 0
            while i < len(lines):
                line = lines[i].strip()
                i += 1

                if not line or len(line) < 2:
                    continue

                # Skip footer/navigation text
                if 'Cita online' in line or 'E-receta' in line or 'La especialidad' in line:
                    continue
                if 'gestionar las solicitudes' in line or 'autorizaciones' in line:
                    continue
                if 'videollamada' in line or 'una cita presencial' in line:
                    continue
                if re.match(r'^[<>]\s*\d+$', line):
                    continue

                # Parse section headers
                city, spec = parse_header(line)
                if city and spec:
                    current_city = city.replace('CAPITAL', '').strip().title()
                    if current_city == 'Asturias Provincia':
                        current_city = ''  # Will be set by subsections
                    current_specialty = spec
                    continue

                # Check for subsection city names (in ASTURIAS PROVINCIA section)
                province_cities = [
                    'AVILÉS', 'AVILES', 'CANGAS DE ONÍS', 'CIAÑO DE LANGREO',
                    'GIJÓN', 'GIJON', 'GRADO', 'LA FELGUERA', 'LLANES', 'LUARCA',
                    'MIERES', 'PIEDRAS BLANCAS', 'POLA DE SIERO', 'SAMA DE LANGREO',
                    'VILLAVICIOSA', 'NAVIA', 'PRAVIA', 'NOREÑA', 'CANGAS DEL NARCEA',
                ]
                if line.upper() in province_cities:
                    current_city = line.strip().title()
                    continue

                # Detect standalone specialty headers (ALL CAPS)
                if re.match(r'^[A-ZÁÉÍÓÚÑÜ\s\.\,\(\)Y/]+$', line) and len(line) > 5:
                    upper = line.upper()
                    if upper not in ('RED DE CENTROS Y', 'PROFESIONALES SANITARIOS',
                                    'CENTROS HOSPITALARIOS', 'ATENCIÓN PRIMARIA',
                                    'ATENCIÓN ESPECIALIZADA', 'MEDIOS COMPLEMENTARIOS DE DIAGNÓSTICO'):
                        current_specialty = line.strip()
                        continue

                # Detect center
                if is_center_line(line):
                    current_center = line.strip()
                    continue

                # Detect person
                if is_person_name(line):
                    # Save previous person
                    if current_person and current_specialty:
                        records.append({
                            'aseguradora': 'DKV',
                            'especialidad_original': current_specialty,
                            'profesional': current_person,
                            'centro': current_center,
                            'direccion': current_address,
                            'telefono': current_phone,
                            'municipio': current_city,
                            'zona': current_city,
                            'pagina_pdf': page_num + 1,
                        })
                    current_person = line.strip()
                    current_address = ''
                    current_phone = ''
                    continue

                # Detect address
                if re.match(r'^(?:C/|Avda\.?|Plaza|Calle|Paseo|Ctra\.|Prao|Naranjo|Quintana|Comandante)', line):
                    current_address = line.strip()
                    continue

                # Detect phone
                if 'Tel.:' in line or 'Tel:' in line:
                    current_phone = extract_phone(line)
                    continue

        # Flush last
        if current_person and current_specialty:
            records.append({
                'aseguradora': 'DKV',
                'especialidad_original': current_specialty,
                'profesional': current_person,
                'centro': current_center,
                'direccion': current_address,
                'telefono': current_phone,
                'municipio': current_city,
                'zona': current_city,
                'pagina_pdf': len(pdf.pages),
            })

    return records

def deduplicate(records):
    seen = set()
    unique = []
    for r in records:
        key = (r['profesional'], r['especialidad_original'], r['municipio'])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique

def main():
    print(f'Extrayendo DKV desde: {PDF_PATH}')
    records = parse_dkv()
    records = deduplicate(records)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    fieldnames = ['aseguradora', 'especialidad_original', 'profesional', 'centro',
                  'direccion', 'telefono', 'municipio', 'zona', 'pagina_pdf']

    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f'Extraídos {len(records)} registros')
    print(f'Guardado en: {OUTPUT_PATH}')

    specialties = set(r['especialidad_original'] for r in records)
    municipios = set(r['municipio'] for r in records if r['municipio'])
    print(f'Especialidades: {len(specialties)}')
    print(f'Municipios: {len(municipios)}')
    for s in sorted(specialties):
        count = sum(1 for r in records if r['especialidad_original'] == s)
        print(f'  {s}: {count}')

if __name__ == '__main__':
    main()
