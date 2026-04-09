#!/usr/bin/env python3
"""
Parse DKV medical directory extracted text into structured CSV.
Handles two-column PDF layout where columns are interleaved on same lines.
"""
import csv
import re
from collections import Counter

INPUT_FILE = r"c:/Users/Asus/Yellow Glasses/Asisa - Documents/Cuadro Médico/data/raw/dkv_fulltext.txt"
OUTPUT_FILE = r"c:/Users/Asus/Yellow Glasses/Asisa - Documents/Cuadro Médico/data/raw/dkv_raw.csv"

KNOWN_CITIES_UPPER = [
    "OVIEDO CAPITAL", "GIJÓN", "AVILÉS", "CANGAS DE ONÍS",
    "CIAÑO DE LANGREO", "GRADO", "LA FELGUERA", "LLANES", "LUARCA",
    "MIERES", "PIEDRAS BLANCAS", "POLA DE SIERO", "POLA DE LENA",
    "SAMA DE LANGREO", "VILLAVICIOSA", "NAVIA", "PRAVIA", "NOREÑA",
    "CANGAS DEL NARCEA", "VEGADEO", "RIBADESELLA",
]

CITY_NORM = {
    "OVIEDO CAPITAL": "Oviedo", "GIJÓN": "Gijón", "AVILÉS": "Avilés",
    "CANGAS DE ONÍS": "Cangas de Onís", "CIAÑO DE LANGREO": "Ciaño de Langreo",
    "GRADO": "Grado", "LA FELGUERA": "La Felguera", "LLANES": "Llanes",
    "LUARCA": "Luarca", "MIERES": "Mieres", "PIEDRAS BLANCAS": "Piedras Blancas",
    "POLA DE SIERO": "Pola de Siero", "POLA DE LENA": "Pola de Lena",
    "SAMA DE LANGREO": "Sama de Langreo", "VILLAVICIOSA": "Villaviciosa",
    "NAVIA": "Navia", "PRAVIA": "Pravia", "NOREÑA": "Noreña",
    "CANGAS DEL NARCEA": "Cangas del Narcea", "VEGADEO": "Vegadeo",
    "RIBADESELLA": "Ribadesella",
}

def normalize_city(city):
    return CITY_NORM.get(city.strip().upper(), city.strip())

SPECIALTIES = [
    "ALERGOLOGÍA", "ANGIOLOGÍA Y CIRUGÍA VASCULAR", "APARATO DIGESTIVO",
    "CARDIOLOGÍA", "CIRUGÍA CARDIOVASCULAR",
    "CIRUGÍA GENERAL Y DEL APARATO DIGESTIVO",
    "CIRUGÍA ORAL Y MÁXILOFACIAL", "CIRUGÍA PEDIÁTRICA",
    "CIRUGÍA PLÁSTICA Y REPARADORA", "CIRUGÍA TORÁCICA",
    "DERMATOLOGÍA Y VENEREOLOGÍA", "ENDOCRINO Y NUTRICIÓN",
    "GINECOLOGÍA", "GINECOLOGÍA Y OBSTETRICIA",
    "HEMATOLOGÍA Y HEMOTERAPIA", "MEDICINA GENERAL", "MEDICINA INTERNA",
    "NEFROLOGÍA", "NEUMOLOGÍA", "NEUROCIRUGÍA", "NEUROLOGÍA",
    "ODONTOLOGÍA GENERAL", "ODONTOLOGÍA IMPLANTES", "ODONTOLOGÍA ORTODONCIA",
    "OFTALMOLOGÍA", "ONCOLOGÍA MÉDICA", "OTORRINOLARINGOLOGÍA",
    "PEDIATRÍA", "PSIQUIATRÍA", "REHABILITACIÓN", "REUMATOLOGÍA",
    "TRAUMATOLOGÍA Y ORTOPEDIA", "UROLOGÍA",
    "ANÁLISIS CLÍNICOS", "ANATOMÍA PATOLÓGICA", "CHEQUEOS MÉDICOS",
    "DENSITOMETRÍA ÓSEA", "DIAGNÓSTICO ESPECIAL PRENATAL",
    "ECOCARDIOGRAFÍA", "ECOGRAFÍA UROLÓGICA", "ERGOMETRÍA", "HOLTER",
    "MAMOGRAFÍA", "NEUROFISIOLOGÍA CLÍNICA", "RADIOLOGÍA CONVENCIONAL",
    "RADIOLOGÍA DENTAL", "RESONANCIA MAGNÉTICA NUCLEAR. R.M.N.",
    "TOMOGRAFÍA AXIAL COMPUTERIZADA. T.A.C.",
    "CHEQUEO MÉDICO GENERAL ANUAL DE PÓLIZA INDIVIDUAL",
    "ENDOSCOPIA DIGESTIVA", "ESTUDIO BIOMECÁNICO DE LA MARCHA",
    "FISIOTERAPIA", "LOGOPEDIA", "PODOLOGÍA",
    "PREPARACIÓN Y RECUPERACIÓN POSTPARTO", "PSICOLOGÍA",
    "ENFERMERÍA", "LASERTERAPIA PROSTÁTICA", "RADIODIAGNÓSTICO",
    "TOMOGRAFÍA AXIAL COMPUTERIZADA.",  # partial version (T.A.C. on next line)
    "RESONANCIA MAGNÉTICA NUCLEAR.",  # partial version (R.M.N. on next line)
]
SPEC_SET = set(s.upper() for s in SPECIALTIES)
# For matching, sort by length descending so longest match wins
SPECS_BY_LEN = sorted(SPECIALTIES, key=len, reverse=True)

SKIP_SECTIONS = {
    "CENTROS HOSPITALARIOS", "HOSPITAL DE DÍA ONCOLÓGICO",
    "HOSPITALIZACIÓN GENERAL", "HOSPITALIZACIÓN PSIQUIÁTRICA",
    "ATENCIÓN PRIMARIA", "ATENCIÓN ESPECIALIZADA",
    "MEDIOS COMPLEMENTARIOS DE DIAGNÓSTICO", "MEDICINA PREVENTIVA",
    "TRATAMIENTOS ESPECIALES",
    "RED DE CENTROS Y", "RED DE CENTROS Y PROFESIONALES SANITARIOS",
    "PROFESIONALES SANITARIOS", "ASTURIAS PROVINCIA",
    "URGENCIAS HOSPITALARIAS", "URGENCIAS AMBULATORIAS", "GENERALES",
    "SERVICIOS DE SALUD Y", "BIENESTAR", "URGENCIAS",
    "DIGESTIVO",  # continuation of multi-line header
    "DIAGNÓSTICO",  # continuation of multi-line header
    "MEDIOS COMPLEMENTARIOS DE",
    "T.A.C.", "R.M.N.",  # continuation of multi-line specialty
}

HOSPITALIZATION = {"HOSPITALIZACIÓN GENERAL", "HOSPITALIZACIÓN PSIQUIÁTRICA", "HOSPITAL DE DÍA ONCOLÓGICO"}

NOISE_RES = [re.compile(p) for p in [
    r"^Cita online:", r"^una cita presencial", r"^autorizaciones en caso",
    r"^La especialidad está disponible", r"^E-receta:", r"^cuidarme Más",
    r"^gestionar las solicitudes", r"^videollamada con esta",
    r"^El profesional sanitario", r"^Consulta virtual:",
    r"Línea Médica", r"^Médico DKV", r"^Fuera de horario",
    r"^SERVICIO DE AMBULANCIAS", r"^976 991 199$", r"^900 810 074",
    r"^URGENCIAS$",
]]

def is_noise(line):
    for r in NOISE_RES:
        if r.search(line):
            return True
    return False

# Center name patterns - known center names from the document
KNOWN_CENTERS = [
    "Centro Médico Asturias", "Centro Médico de Asturias", "Centro Medico San Rafael",
    "Clínica Asturias", "Clínica El Fontán", "Clínica Ayala", "Clínica Cervantes",
    "Clínica El Fontán", "Clínica Garaya", "Clínica Gijón", "Clínica Junquera",
    "Clínica Santa Susana", "Policlínicas Oviedo", "Policlínica Oviedo",
    "Clínica Asturias", "Instituto Rehavitall", "Maxilo - Astur",
    "Join Dental", "Guillermo Rehberger Olivera", "Rehberger López - Fanjul",
    "Clínica Bucodental", "Clínica Oftalmológica Dres. Bascarán",
    "Clínica Oftalmológica Bascarán", "Radiología Asturiana",
    "ATRYS Smartcare", "Laboratorio Echevarne", "Laboratorio Bioquim",
    "Synlab Oviedo", "Citología y Técnicas Aplicadas A.P.",
    "Merino Laboratorio", "Fisioterapia Integral Sierra", "Psicastur",
    "Centro Médico Por Tú Salud", "Clínica B Internacional",
    "Policlínica Rozona", "Previtalia", "Salutec", "Cenac",
    "Clínica Médica Dr. Marcos Barrientos", "Clínica de Fisioterapia José Feito",
    "Instituto Rehabilitación Astur", "Clínica Cangas",
    "Sanatorio Adaro", "Centro Médico Gijón", "Clínica San Ezequiel",
    "Policlínica Begoña", "Centro Médico Salud 4", "Clínica Luanco",
    "Hospital Begoña", "Hospital Covadonga", "Gabinete Odontológico",
    "Centro Médico Muñoz", "Clínica El Molinón", "Clínica Dental Balgos",
    "Multiclínicas Laserdental", "Corporación Fisiogestión Gijón",
    "Fisiocenter", "Centro APS Fisioterapia", "Clínica Zigomat",
    "Clínicas Cardialis", "Clínica Castillo de Llanes", "Clínica Llanes",
    "Hospital de Luarca", "Clínica Kineastur", "Clínica Médica Semad",
    "Clínica Principal", "Centro de Fisioterapia Eduardo Álvarez",
    "Clinica Lena", "Clínica Rodríguez Reguero", "Clínica Sella",
    "Laboratorio Covadonga Gutiérrez",
    "Clínica Baviera", "Clínica Gijón Dental",
    "Laboratorio Covadonga", "Laboratorio Dr. Oliver",
    "Insituto Urológico Asturiano", "Instituto Urológico Asturiano",
    "Linares Espec. En Digestivo", "Clínica Dental Ladent",
    "Vitaldent", "Clínica Soma", "Fisioterapia Mieres",
    "Grupo Dental Ortega", "Hospital de Jarrio",
    "Clínica Oftalmológica Dres.", "Imagen Diagnóstica El Molinón",
    "Clínica Imagen Diagnóstica de", "Clínica Dental Rubió e Hijo",
    "Clínica Dental Rubió", "Es Clínic",
]
# Build set for quick lookup
KNOWN_CENTERS_SET = set(c.upper() for c in KNOWN_CENTERS)


def is_name_str(s):
    """Check if s matches Surname(s), FirstName(s) pattern."""
    s = s.strip()
    if ',' not in s:
        return False
    if s == s.upper():
        return False
    parts = s.split(',', 1)
    before = parts[0].strip()
    after = parts[1].strip()
    if not before or not after:
        return False
    if not re.search(r'[a-záéíóúñA-Z]', before):
        return False
    if not re.search(r'[a-záéíóúñA-Z]', after):
        return False
    if re.match(r'^(C/|Avda\.|Ctra\.|Tel\.|Plaza )', s):
        return False
    first_word = before.split()[0] if before.split() else ""
    if not first_word or not first_word[0].isupper():
        return False
    # Filter out address-like: "S/N" after comma, or pure digits, or "Bajo", "1º", etc.
    # Allow "0scar" type OCR errors (digit followed by letters)
    if after.startswith('S/N'):
        return False
    if re.match(r'^\d+$', after) or re.match(r'^\d+[\s\-]', after):
        return False
    if re.match(r'^(Bajo|Bajos|Entlo|Entreplanta|1[ºª]|2[ºª]|3[ºª])', after):
        return False
    # Filter out things like "Buenavista 4, Bajo"
    if re.search(r'\d', before) and re.match(r'^(Bajo|Bajos|Entlo|1|2|3)', after):
        return False
    return True


def is_address_str(s):
    """Check if s starts with an address pattern."""
    return bool(re.match(
        r'^(C/|Avda\.|Ctra\.|Plaza |Paseo |Trav\.|Quintana[ ,\d]|Matemático|Comandante |Valentín |San Francisco |Asturias \d|Cervantes \d|Jovellanos |Buenavista |Dr\. Hurlé)',
        s.strip()
    ))


def is_phone_str(s):
    return bool(re.match(r'^Tel\.?:\s*', s.strip()))


def split_line(line):
    """
    Split a two-column merged line into separate fragments.
    Returns list of strings.
    """
    line = line.strip()
    if not line:
        return []

    fragments = []

    # Strategy: scan from left to right, detect boundaries between column content
    # Key boundary indicators:
    # 1. A name pattern (Surname, Name) appearing after a center name, address, or another name
    # 2. An address (C/, Avda.) appearing after a name or center name
    # 3. A center name appearing after a phone number or address
    # 4. Tel.: appearing mid-line
    # 5. A specialty in ALL CAPS appearing mid-line

    # First, split on Tel.: which is a clear separator
    tel_parts = re.split(r'(?=Tel\.?:\s*\d)', line)
    for tp in tel_parts:
        tp = tp.strip()
        if not tp:
            continue
        if is_phone_str(tp):
            # Could have more content after the phone
            # Phone number: digits, spaces, hyphens, slashes
            m = re.match(r'^(Tel\.?:\s*[\d\s\-/]+)(.*)', tp)
            if m:
                fragments.append(m.group(1).strip())
                rest = m.group(2).strip()
                if rest:
                    fragments.extend(split_non_phone(rest))
            else:
                fragments.append(tp)
        else:
            fragments.extend(split_non_phone(tp))

    return fragments


def split_non_phone(text):
    """Split a text fragment that doesn't start with Tel.: into sub-fragments."""
    text = text.strip()
    if not text:
        return []

    results = []

    # Try to find specialty keywords (ALL CAPS multi-word) in the middle of text
    for spec in SPECS_BY_LEN:
        spec_up = spec.upper()
        idx = text.upper().find(spec_up)
        if idx > 0:
            before = text[:idx].strip()
            after_start = idx + len(spec)
            after = text[after_start:].strip() if after_start < len(text) else ""
            if before:
                results.extend(split_non_phone(before))
            results.append(spec)
            if after:
                results.extend(split_non_phone(after))
            return results
        elif idx == 0 and len(text) > len(spec) + 1:
            after = text[len(spec):].strip()
            if after and not after[0].isalpha():
                # Probably not a merged line
                continue
            results.append(spec)
            if after:
                results.extend(split_non_phone(after))
            return results

    # Try to split on name patterns appearing after other content
    # Look for "Surname Surname, Name" patterns mid-line
    # Name pattern: capital letter word(s) followed by comma then name
    # Be careful: addresses also have commas (C/ xxx, 11)
    name_re = re.compile(
        r'(?<=\s)([A-ZÁÉÍÓÚÑ][a-záéíóúñA-ZÁÉÍÓÚÑ\s\-]+,\s*[A-ZÁÉÍÓÚÑ][a-záéíóúñ\s\.]+?)(?=\s+[A-ZÁÉÍÓÚÑ]|\s*$)'
    )

    # Check for section headers mixed into content (from two-column merge)
    section_patterns = [
        "ATENCIÓN PRIMARIA", "ATENCIÓN ESPECIALIZADA",
        "MEDIOS COMPLEMENTARIOS DE", "TRATAMIENTOS ESPECIALES",
        "MEDICINA PREVENTIVA", "DIAGNÓSTICO",
    ]
    for sp in section_patterns:
        idx = text.find(sp)
        if idx > 0:
            before = text[:idx].strip()
            if before:
                results.extend(split_non_phone(before))
            # Rest is section header noise - skip
            return results if results else []
        elif idx == 0 and len(text) > len(sp) + 1:
            after = text[len(sp):].strip()
            if after:
                results.extend(split_non_phone(after))
            return results if results else []

    # Check for city names at the START of text followed by name content (from two-column merge)
    # Only at start to avoid false matches in addresses like "Ctra. de Grado, S/N"
    for city in sorted(KNOWN_CITIES_UPPER, key=len, reverse=True):
        up_text = text.upper()
        if up_text.startswith(city + " "):
            city_text = text[:len(city)]
            after = text[len(city):].strip()
            results.append(city_text)
            if after:
                results.extend(split_non_phone(after))
            return results
    # Also check for city at END of text (e.g., "Name, FirstName SAMA DE LANGREO")
    for city in sorted(KNOWN_CITIES_UPPER, key=len, reverse=True):
        up_text = text.upper()
        if up_text.endswith(" " + city):
            before = text[:-(len(city))].strip()
            city_text = text[-(len(city)):]
            if before:
                results.extend(split_non_phone(before))
            results.append(city_text)
            return results

    # Check for Línea Médica noise mixed into content
    lm_idx = text.find("Línea Médica")
    if lm_idx >= 0:
        before = text[:lm_idx].strip()
        if before:
            results.extend(split_non_phone(before))
        # Everything from Línea Médica onward is noise, skip it
        return results if results else [text[:lm_idx].strip()] if text[:lm_idx].strip() else []

    # Try to find a center name appearing mid-line (BEFORE address split, to handle
    # cases like "CenterName Surname, Name" where center includes special chars)
    for center in sorted(KNOWN_CENTERS, key=len, reverse=True):
        center_up = center.upper()
        idx = text.upper().find(center_up)
        if idx >= 0 and (idx > 0 or len(text) > len(center) + 1):
            before = text[:idx].strip() if idx > 0 else ""
            center_text = text[idx:idx+len(center)].strip()
            after = text[idx+len(center):].strip()
            if not before and not after:
                continue  # exact match, no split needed
            if before:
                results.extend(split_non_phone(before))
            results.append(center_text)
            if after:
                results.extend(split_non_phone(after))
            return results

    # Try to find address patterns (C/, Avda.) appearing mid-line
    addr_re = re.compile(r'(?<=\s)(C/|Avda\.|Ctra\.|Plaza |Paseo |Trav\.|Quintana[ ,\d]|Jovellanos |Buenavista |Dr\. Hurlé)')
    m_addr = addr_re.search(text)
    if m_addr and m_addr.start() > 3:
        before = text[:m_addr.start()].strip()
        after = text[m_addr.start():].strip()
        if before and (is_name_str(before) or before[0].isupper()):
            results.extend(split_non_phone(before))
            results.extend(split_non_phone(after))
            return results

    # Try to find two names merged: "Surname1, Name1 Surname2, Name2"
    # Look for comma patterns
    commas = [m.start() for m in re.finditer(r',', text)]
    if len(commas) >= 2:
        # Try every possible split point between two commas
        for ci in range(len(commas) - 1):
            c1 = commas[ci]
            c2 = commas[ci + 1]
            # The text between the two commas contains: "FirstName1 Surname2"
            between = text[c1+1:c2]
            words = between.split()
            # Try splitting after each word
            for wi in range(1, len(words)):
                first_part = text[:c1+1] + " ".join(words[:wi])
                second_part = " ".join(words[wi:]) + text[c2:]
                if is_name_str(first_part.strip()) and is_name_str(second_part.strip()):
                    results.append(first_part.strip())
                    results.append(second_part.strip())
                    return results

    # No split found, return as-is
    return [text]


def classify_token(text):
    """Classify a text fragment into a token type."""
    text = text.strip()
    if not text:
        return None

    up = text.upper()

    if is_noise(text):
        return None

    if is_phone_str(text):
        phone = re.sub(r'^Tel\.?:\s*', '', text).strip()
        return ('PHONE', phone)

    if up in SPEC_SET:
        return ('SPECIALTY', text)

    if up in {s.upper() for s in SKIP_SECTIONS}:
        return ('SECTION', text)

    if up in {h.upper() for h in HOSPITALIZATION}:
        return ('HOSP', text)

    for c in KNOWN_CITIES_UPPER:
        if up == c:
            return ('CITY', text)

    # Check for merged city + specialty
    for c in sorted(KNOWN_CITIES_UPPER, key=len, reverse=True):
        if up.startswith(c + " "):
            remainder = up[len(c):].strip()
            if remainder in SPEC_SET:
                return ('CITY_SPEC', (c, text[len(c):].strip()))
            # Check if remainder is another city (two cities on same line from 2 columns)
            for c2 in KNOWN_CITIES_UPPER:
                if remainder == c2:
                    # Two cities merged - return both as separate tokens
                    return ('CITY_CITY', (c, c2))

    if is_address_str(text):
        return ('ADDRESS', text)

    if is_name_str(text):
        return ('NAME', text)

    # Check against known centers
    if up in KNOWN_CENTERS_SET:
        return ('CENTER', text)

    # Check for center-like names (starts with capital, could be a clinic/center)
    if text[0].isupper() and not re.match(r'^\d', text):
        # Heuristic: if line is relatively short and titlecase-ish, probably center
        if len(text) < 80:
            return ('CENTER', text)

    return None


def main():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        raw_text = f.read()

    lines = raw_text.split('\n')

    # Phase 1: Pre-process lines into clean tokens
    tokens = []
    current_page = 0

    for line in lines:
        orig = line.strip()
        if not orig:
            continue

        # Page marker
        m = re.match(r'^=== PAGINA (\d+) ===$', orig)
        if m:
            current_page = int(m.group(1))
            tokens.append(('PAGE', current_page))
            continue

        # Skip pages outside range
        if current_page < 24 or current_page >= 76:
            continue

        # Section header
        m = re.match(r'^>\s*(.+?)\s*\|\s*(.+?)\s*<$', orig)
        if m:
            tokens.append(('HEADER', (m.group(1).strip(), m.group(2).strip())))
            continue

        # Page markers
        if re.match(r'^[><]\s*\d+$', orig):
            continue
        if re.match(r'^>.*<$', orig):
            continue

        if is_noise(orig):
            continue

        # Split the line into fragments (handle two-column merge)
        frags = split_line(orig)
        for frag in frags:
            tok = classify_token(frag)
            if tok:
                tokens.append(tok)

    # Phase 2: Build records from token stream
    records = []
    current_page = 0
    current_city = None
    current_specialty = None
    current_center = None
    current_address = None
    current_phone = None
    in_hospitalization = False
    pending_names = []

    def flush():
        nonlocal pending_names
        if pending_names and current_specialty:
            for name in pending_names:
                records.append([
                    "DKV", current_specialty, name,
                    current_center or "", current_address or "",
                    current_phone or "", current_city or "",
                    "Asturias", current_page,
                ])
        pending_names = []

    for tok in tokens:
        ttype = tok[0]
        tval = tok[1]

        if ttype == 'PAGE':
            current_page = tval
            continue

        if ttype == 'HEADER':
            part1, part2 = tval
            p1u = part1.upper()
            p2u = part2.upper()
            for c in KNOWN_CITIES_UPPER:
                if p1u == c:
                    flush()
                    current_city = normalize_city(c)
                    in_hospitalization = False
                    current_center = None
                    current_address = None
                    current_phone = None
                    break
                if p2u == c:
                    flush()
                    current_city = normalize_city(c)
                    in_hospitalization = False
                    current_center = None
                    current_address = None
                    current_phone = None
                    break
            continue

        if ttype == 'CITY':
            flush()
            current_city = normalize_city(tval)
            in_hospitalization = False
            current_center = None
            current_address = None
            current_phone = None
            continue

        if ttype == 'CITY_SPEC':
            flush()
            city_name, spec_name = tval
            current_city = normalize_city(city_name)
            for s in SPECIALTIES:
                if spec_name.upper() == s.upper():
                    current_specialty = s
                    break
            in_hospitalization = False
            current_center = None
            current_address = None
            current_phone = None
            continue

        if ttype == 'CITY_CITY':
            # Two cities merged from two columns - use the second one (right column)
            # since the header usually corresponds to the right column city
            flush()
            _, city2 = tval
            current_city = normalize_city(city2)
            in_hospitalization = False
            current_center = None
            current_address = None
            current_phone = None
            continue

        if ttype == 'HOSP':
            flush()
            in_hospitalization = True
            current_specialty = None
            current_center = None
            current_address = None
            current_phone = None
            continue

        if ttype == 'SECTION':
            continue

        if ttype == 'SPECIALTY':
            flush()
            spec_val = tval.strip()
            # Normalize partial specialty names
            spec_norm = {
                "TOMOGRAFÍA AXIAL COMPUTERIZADA.": "TOMOGRAFÍA AXIAL COMPUTERIZADA. T.A.C.",
                "RESONANCIA MAGNÉTICA NUCLEAR.": "RESONANCIA MAGNÉTICA NUCLEAR. R.M.N.",
            }
            if spec_val.upper() in spec_norm:
                spec_val = spec_norm[spec_val.upper()]
            else:
                for s in SPECIALTIES:
                    if spec_val.upper() == s.upper():
                        spec_val = s
                        break
            current_specialty = spec_val
            in_hospitalization = False
            current_center = None
            current_address = None
            current_phone = None
            continue

        if in_hospitalization:
            continue

        if not current_specialty:
            continue

        if ttype == 'CENTER':
            flush()
            current_center = tval
            current_address = None
            current_phone = None
            continue

        if ttype == 'NAME':
            pending_names.append(tval)
            continue

        if ttype == 'ADDRESS':
            current_address = tval
            continue

        if ttype == 'PHONE':
            current_phone = tval
            flush()
            continue

    flush()

    # Write CSV
    with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['aseguradora', 'especialidad_original', 'profesional', 'centro', 'direccion', 'telefono', 'municipio', 'zona', 'pagina_pdf'])
        for r in records:
            writer.writerow(r)

    print(f"Written {len(records)} records to {OUTPUT_FILE}")

    # Stats
    cities = Counter(r[6] for r in records)
    specs = Counter(r[1] for r in records)
    print(f"\nMunicipios ({len(cities)}):")
    for c, n in cities.most_common():
        print(f"  {c}: {n}")
    print(f"\nEspecialidades ({len(specs)}):")
    for s, n in specs.most_common():
        print(f"  {s}: {n}")

    # Check for suspicious records
    print("\n--- Quality checks ---")
    issues = 0
    for r in records:
        name = r[2]
        if len(name) > 55:
            print(f"  LONG NAME: [{r[1]}] {name}")
            issues += 1
        if 'Tel.' in name or 'C/' in name or 'Avda.' in name:
            print(f"  ADDR/PHONE IN NAME: [{r[1]}] {name}")
            issues += 1
        if issues > 30:
            print("  ... (truncated)")
            break

    # Show records with empty center
    no_center = sum(1 for r in records if not r[3])
    no_addr = sum(1 for r in records if not r[4])
    no_phone = sum(1 for r in records if not r[5])
    print(f"\n  Records without center: {no_center}")
    print(f"  Records without address: {no_addr}")
    print(f"  Records without phone: {no_phone}")


if __name__ == "__main__":
    main()
