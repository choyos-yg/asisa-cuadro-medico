#!/usr/bin/env python3
"""
Parse Adeslas medical directory extracted text into structured CSV.
Handles two-column layout where left and right columns are merged per line.
"""
import csv
import re

INPUT = r"c:/Users/Asus/Yellow Glasses/Asisa - Documents/Cuadro Médico/data/raw/adeslas_fulltext.txt"
OUTPUT = r"c:/Users/Asus/Yellow Glasses/Asisa - Documents/Cuadro Médico/data/raw/adeslas_raw.csv"

with open(INPUT, "r", encoding="utf-8") as f:
    raw_lines = f.readlines()

# --- Step 1: Split into pages ---
pages = {}
current_page = None
for line in raw_lines:
    line = line.rstrip("\n").rstrip("\r")
    m = re.match(r"=== PAGINA (\d+) ===", line)
    if m:
        current_page = int(m.group(1))
        pages[current_page] = []
    elif current_page is not None:
        pages[current_page].append(line)

# --- Specialties (sorted by length desc for greedy matching) ---
VALID_SPECIALTIES = sorted([
    "TRAUMATOLOGÍA Y CIRUGÍA ORTOPÉDICA",
    "CIRUGÍA GENERAL Y DEL APARATO DIGESTIVO",
    "CIRUGÍA GENERAL Y DEL AP. DIGESTIVO",
    "ANGIOLOGÍA Y CIRUGÍA VASCULAR",
    "ANESTESIOLOGÍA Y REANIMACIÓN",
    "CIRUGÍA PLÁSTICA Y REPARADORA",
    "DERMATOLOGÍA Y VENEREOLOGÍA",
    "MICROBIOLOGÍA Y PARASITOLOGÍA",
    "GINECOLOGÍA Y OBSTETRICIA",
    "HEMATOLOGÍA Y HEMOTERAPIA",
    "ENDOCRINOLOGÍA Y NUTRICIÓN",
    "ESTOMATOLOGÍA (ODONTOLOGÍA)",
    "ESTOMATOLOGÍA (ODONTOLOGíA)",
    "DIETÉTICA Y NUTRICIÓN",
    "CIRUGÍA CARDIOVASCULAR",
    "ONCOLOGÍA RADIOTERÁPICA",
    "TRATAMIENTO DEL DOLOR",
    "PREPARACIÓN AL PARTO",
    "CIRUGÍA MAXILOFACIAL",
    "ANATOMÍA PATOLÓGICA",
    # "PERSONAL SANITARIO" removed - it's a section divider, not a specialty
    "ONCOLOGÍA MÉDICA",
    "MEDICINA NUCLEAR",
    "MEDICINA INTERNA",
    "MEDICINA GENERAL",
    "APARATO DIGESTIVO",
    "OTORRINOLARINGOLOGÍA",
    "ANÁLISIS CLÍNICOS",
    "NEUROFISIOLOGÍA",
    "RADIODIAGNÓSTICO",
    "REHABILITACIÓN",
    "REUMATOLOGÍA",
    "NEUROCIRUGÍA",
    "OFTALMOLOGÍA",
    "ALERGOLOGÍA",
    "CARDIOLOGÍA",
    "ENFERMERÍA",
    "FISIOTERAPIA",
    "NEUMOLOGÍA",
    "NEUROLOGÍA",
    "NEFROLOGÍA",
    "PROCTOLOGÍA",
    "PSIQUIATRÍA",
    "PSICOLOGÍA",
    "PEDIATRÍA",
    "PODOLOGÍA",
    "AUDIOLOGÍA",
    "LOGOPEDIA",
    "MATRONAS",
    "UROLOGÍA",
    "ENDOSCOPIAS",
    "ESTOMATOLOGÍA",  # sometimes split as "ESTOMATOLOGÍA\n(ODONTOLOGÍA)"
    "CIRUGÍA GENERAL",
    "CIRUGÍA ORTOPÉDICA",  # continuation of "TRAUMATOLOGÍA Y"
    "TRAUMATOLOGÍA Y",     # first part
], key=len, reverse=True)

VALID_SPECIALTIES_SET = set(VALID_SPECIALTIES)

SKIP_KEYWORDS = [
    "URGENCIAS AMBULATORIAS",
    "SERVICIO DE ATENCIÓN MÉDICA",
    "URGENCIAS EXTRAHOSPITALARIAS",
    "URGENCIAS HOSPITALARIAS",
    "URGENCIAS ENFERMERÍA",
    "ATENCIÓN ESPECIALIZADA",
    "SERVICIO DE URGENCIA",
    "URGENCIAS MÉDICAS",
    "ATENCIÓN URGENCIAS",
    "ATENCIÓN PRIMARIA",
    "CONSULTAS EXTERNAS",
    "AMBULANCIAS TRANSINSA",
    "AMBULANCIAS",
    "URGENCIAS",
    "PERSONAL SANITARIO",
]

# Municipality names and sub-localities (sorted by length desc)
MUNICIPIO_ENTRIES = sorted([
    "SAN MARTÍN DEL REY AURELIO",
    "SANTA EULALIA DE OSCOS",
    "BELMONTE DE MIRANDA",
    "CORVERA DE ASTURIAS",
    "TAPIA DE CASARIEGO",
    "CARREÑA DE CABRALES",
    "ARENAS DE CABRALES",
    "POSADA DE LLANERA",
    "VILLANUEVA DE PRÍA",
    "MIERES DEL CAMINO",
    "POSADA DE LLANES",
    "NUEVA DE LLANES",
    "CANGAS DEL NARCEA",
    "CANGAS DE ONÍS",
    "SOTO DEL BARCO",
    "POLA DE SIERO",
    "POLA DE LENA",
    "PIEDRAS BLANCAS",
    "LA FRESNEDA",
    "LA CARIDAD",
    "RIBADESELLA",
    "RIBADEDEVA",
    "CASTRILLÓN",
    "VILLAVICIOSA",
    "VILLAMAYOR",
    "CASTROPOL",
    "COLOMBRES",
    "ARRIONDAS",
    "SOTRONDIO",
    "CUDILLERO",
    "INFIESTO",
    "CABRALES",
    "CARREÑO",
    "COLUNGA",
    "LANGREO",
    "LAVIANA",
    "LLANERA",
    "LLANES",
    "NOREÑA",
    "PILOÑA",
    "PRAVIA",
    "VALDÉS",
    "LUARCA",
    "AVILÉS",
    "MOREDA",
    "CANDÁS",
    "LUANCO",
    "GOZÓN",
    "GRADO",
    "TURÓN",
    "ALLER",
    "MIERES",
    "SALAS",
    "SIERO",
    "LENA",
    "NAVA",
    "NAVIA",
    "ONÍS",
    "BOAL",
    "SAMA",
    "LASTRES",
    "TINEO",
    "PARRES",
    "SOMIEDO",
    "VEGADEO",
    "BELMONTE",
    "CORVERA",
    "CARAVIA",
    "FRANCO (EL)",
], key=len, reverse=True)

# Map all municipality names to their normalized output name
MUNICIPIO_MAP = {
    "MOREDA": "Moreda de Aller", "ALLER": "Aller",
    "SAMA": "Sama de Langreo", "LANGREO": "Langreo",
    "LA FRESNEDA": "La Fresneda",
    "CANDÁS": "Candás", "CARREÑO": "Carreño",
    "LUANCO": "Luanco", "GOZÓN": "Gozón",
    "LASTRES": "Lastres", "COLUNGA": "Colunga",
    "LA CARIDAD": "La Caridad", "FRANCO (EL)": "El Franco",
    "ARENAS DE CABRALES": "Arenas de Cabrales", "CABRALES": "Cabrales",
    "CARREÑA DE CABRALES": "Cabrales",
    "POLA DE SIERO": "Pola de Siero", "SIERO": "Siero",
    "POLA DE LENA": "Pola de Lena", "LENA": "Lena",
    "POSADA DE LLANERA": "Posada de Llanera", "LLANERA": "Llanera",
    "PIEDRAS BLANCAS": "Piedras Blancas", "CASTRILLÓN": "Castrillón",
    "COLOMBRES": "Colombres", "RIBADEDEVA": "Ribadedeva",
    "ARRIONDAS": "Arriondas", "PARRES": "Parres",
    "INFIESTO": "Infiesto", "PILOÑA": "Piloña",
    "SOTRONDIO": "Sotrondio", "SAN MARTÍN DEL REY AURELIO": "San Martín del Rey Aurelio",
    "BELMONTE": "Belmonte de Miranda", "BELMONTE DE MIRANDA": "Belmonte de Miranda",
    "CORVERA DE ASTURIAS": "Corvera de Asturias", "CORVERA": "Corvera de Asturias",
    "TAPIA DE CASARIEGO": "Tapia de Casariego",
    "MIERES DEL CAMINO": "Mieres", "MIERES": "Mieres",
    "CANGAS DEL NARCEA": "Cangas del Narcea",
    "CANGAS DE ONÍS": "Cangas de Onís",
    "SOTO DEL BARCO": "Soto del Barco",
    "SANTA EULALIA DE OSCOS": "Santa Eulalia de Oscos",
    "AVILÉS": "Avilés", "LLANES": "Llanes", "LANGREO": "Langreo",
    "LAVIANA": "Laviana", "NOREÑA": "Noreña",
    "PRAVIA": "Pravia", "VALDÉS": "Valdés", "LUARCA": "Luarca",
    "RIBADESELLA": "Ribadesella", "SALAS": "Salas",
    "GRADO": "Grado", "TINEO": "Tineo",
    "SOMIEDO": "Somiedo", "VEGADEO": "Vegadeo",
    "VILLAVICIOSA": "Villaviciosa", "NAVA": "Nava",
    "NAVIA": "Navia", "ONÍS": "Onís", "BOAL": "Boal",
    "CASTROPOL": "Castropol", "CUDILLERO": "Cudillero",
    "CARAVIA": "Caravia", "TURÓN": "Mieres",
    "VILLANUEVA DE PRÍA": "Llanes", "POSADA DE LLANES": "Llanes",
    "NUEVA DE LLANES": "Llanes", "VILLAMAYOR": "Piloña",
    "PARRES": "Parres",
}

CENTER_KEYWORDS = [
    "POLICLÍNICAS OVIEDO", "POLICLÍNICAS GIJÓN",
    "HOSPITAL BEGOÑA", "HOSPITAL DE JOVE",
    "CLÍNICA ASTURIAS", "CLÍNICA CERVANTES", "CLÍNICA SAN RAFAEL",
    "CENTRO MÉDICO DE ASTURIAS", "CENTRO MÉDICO",
    "CLÍNICA PSIQUIÁTRICA SOMIÓ",
    "SANATORIO ADARO", "POLICLÍNICA ROZONA",
    "SYNLAB", "ECHEVARNE",
    "LABORATORIO DE ANÁLISIS CLÍNICOS",
    "POLICLÍNICAS OVIEDO-ANÁLISIS CLÍNICOS",
]

# --- Helper functions ---

ADDRESS_PREFIXES = r'(?:Avda|Avda\.|C/|Plaza|Pza|Paseo|Pº|Ctra|Camino|Calle|Prao)'

NAME_PATTERN = re.compile(
    r'([A-ZÁÉÍÓÚÜÑÇÈ][A-ZÁÉÍÓÚÜÑÇÈ\s\-\'\.]{2,}?'
    r',\s*'
    # First name: one or more words. Use lookahead to stop before address patterns.
    # An address pattern is: Word, digit (like "Cervantes, 17")
    r'(?:[A-ZÁÉÍÓÚÜÑÇa-záéíóúüñçè][a-záéíóúüñçèª\.]*'
    r'(?:\s+(?:de\s+(?:la\s+|los\s+|las\s+)?)?[A-ZÁÉÍÓÚÜÑÇa-záéíóúüñçè][a-záéíóúüñçèª\.]+)*?)'
    # Lookahead: stop if followed by various patterns
    r'(?=\s+[A-ZÁÉÍÓÚÜÑÇa-záéíóúüñçè][a-záéíóúüñçè]*,\s*\d'  # address: Word, digit
    r'|\s+[A-ZÁÉÍÓÚÜÑÇÈ]{2,}\s'  # ALL-CAPS word (another surname)
    r'|\s+[A-ZÁÉÍÓÚÜÑÇÈ]{2,}$'   # ALL-CAPS word at end
    r'|\s+' + ADDRESS_PREFIXES + r'[\s\.]'  # address prefix words
    r'|\s*$'                        # end of string
    r'))'
)

def clean_name(name):
    """Clean a name by removing trailing address/location content."""
    parts = name.split(",", 1)
    if len(parts) != 2:
        return name
    surname = parts[0].strip()
    firstname = parts[1].strip()

    words = firstname.split()
    connectors = {"de", "del", "la", "los", "las", "el"}

    # Count meaningful name words
    name_word_count = sum(1 for w in words if w.lower() not in connectors)

    # Spanish first names: typically 1-2 substantial words
    # (José, José María, Ángela de la Cruz, Mª Eugenia, Fco. Jesús)
    if name_word_count <= 2:
        return name

    # For 3+ substantial words, keep only first 2 substantial words (plus connectors)
    kept = []
    substantial_count = 0
    for i, w in enumerate(words):
        if w.lower() in connectors:
            kept.append(w)
            continue
        if w.endswith('.') or w == "Mª":
            kept.append(w)
            substantial_count += 1
            continue
        substantial_count += 1
        if substantial_count > 2:
            break
        kept.append(w)

    if kept:
        # Remove trailing connectors
        while kept and kept[-1].lower() in connectors:
            kept.pop()
        return surname + ", " + " ".join(kept)
    return name

def find_names_in_text(text):
    """Find all professional names in text."""
    found = []
    for m in NAME_PATTERN.finditer(text):
        raw_name = m.group(1).strip().rstrip('.')
        name = clean_name(raw_name)
        parts = name.split(",", 1)
        if len(parts) != 2:
            continue
        surname = parts[0].strip()
        firstname = parts[1].strip()
        if not surname or not firstname or len(firstname) < 2:
            continue
        if not re.match(r'^[A-ZÁÉÍÓÚÜÑÇÈ][A-ZÁÉÍÓÚÜÑÇÈ\s\-\'\.]+$', surname):
            continue
        if any(kw in name.upper() for kw in ["TELÉFONO", "CONSULTA", "POLICLÍNICA",
            "HOSPITAL", "CLÍNICA", "CENTRO MÉD", "AMBULANCIA", "SANATORIO"]):
            continue
        # Recalculate end position based on cleaned name
        actual_end = m.start() + len(name)
        found.append((m.start(), actual_end, name))
    return found

def find_all_specs_in_text(text):
    """Find all specialty occurrences in text with positions."""
    found = []
    for spec in VALID_SPECIALTIES:
        idx = 0
        while True:
            idx = text.find(spec, idx)
            if idx == -1:
                break
            end = idx + len(spec)
            before_ok = (idx == 0 or not text[idx-1].isalpha())
            after_ok = (end >= len(text) or not text[end].isalpha())
            if before_ok and after_ok:
                overlap = any(idx < fe and end > fs for fs, fe, _ in found)
                if not overlap:
                    found.append((idx, end, spec))
            idx += 1
    found.sort(key=lambda x: x[0])
    return found

def find_municipio_at_start(text):
    """Check if text starts with a municipality name."""
    text_stripped = text.strip()
    for muni in MUNICIPIO_ENTRIES:
        if text_stripped == muni:
            return muni, ""
        if text_stripped.startswith(muni + " ") or text_stripped.startswith(muni + "\t"):
            return muni, text_stripped[len(muni):].strip()
    return None, text_stripped

def find_skip_at_start(text):
    """Check if text starts with a skip keyword."""
    text = text.strip()
    for skip in SKIP_KEYWORDS:
        if text == skip or text.startswith(skip + " ") or text.startswith(skip + "\t"):
            return True
    return False

def is_phone_line(text):
    return bool(re.match(r'^(?:Teléfono|Teléfonos|Tfno)', text.strip()))

def extract_phone(text):
    text = text.strip()
    text = re.sub(r'^(?:Teléfono|Teléfonos|Tfno\.?)\s*', '', text)
    text = re.sub(r'\s*(?:Consulta|previa|Horario|servicio).*', '', text, flags=re.IGNORECASE)
    phones = re.findall(r'\d[\d\s]{7,}', text)
    return " / ".join(p.strip() for p in phones) if phones else ""

def is_center(text):
    text = text.strip()
    for cn in CENTER_KEYWORDS:
        if text.upper().startswith(cn.upper()):
            return True
    return False

def is_consultation_note(text):
    text = text.strip().lower()
    keywords = ['consulta ', 'previa petición', 'horario de',
                'ininterrumpidamente', 'servicio a domicilio',
                'y laborables', 'diurnas', 'nocturnos',
                'fines de semana', 'urgencias ambulatorias',
                'horario de invierno', 'horario de verano',
                'extracciones de']
    return any(kw in text for kw in keywords)

def is_schedule_fragment(text):
    text = text.strip().lower()
    if re.match(r'^(?:de |y de |mañanas|tardes)\d', text.replace(' ', '')):
        return True
    if re.match(r'^(?:de |y de )\d', text):
        return True
    if re.match(r'^(?:lunes|martes|miércoles|jueves|viernes|sábados|domingos)', text):
        return True
    return False

def is_index_page(lines):
    return sum(1 for l in lines if '.....' in l) > 5

def normalize_specialty(spec):
    if spec in ("CIRUGÍA GENERAL Y DEL AP. DIGESTIVO", "CIRUGÍA GENERAL"):
        return "CIRUGÍA GENERAL Y DEL APARATO DIGESTIVO"
    if spec == "ESTOMATOLOGÍA (ODONTOLOGíA)":
        return "ESTOMATOLOGÍA (ODONTOLOGÍA)"
    if spec == "ESTOMATOLOGÍA":
        return "ESTOMATOLOGÍA (ODONTOLOGÍA)"
    if spec == "TRAUMATOLOGÍA Y":
        return "TRAUMATOLOGÍA Y CIRUGÍA ORTOPÉDICA"
    if spec == "CIRUGÍA ORTOPÉDICA":
        return "TRAUMATOLOGÍA Y CIRUGÍA ORTOPÉDICA"
    return spec

# --- Page classification ---
OVIEDO_DETAIL = set(range(10, 25))
GIJON_DETAIL = set(range(28, 45))
MUNI_DETAIL = set(range(46, 73))
SUMMARY_PAGES = {2, 3, 9, 27}

def get_section(page_num):
    if page_num in OVIEDO_DETAIL: return "Oviedo"
    if page_num in GIJON_DETAIL: return "Gijón"
    if page_num in MUNI_DETAIL: return "Municipios"
    return None

# --- Main parsing ---
results = []
current_specialty = None
current_municipio = None
current_zona = None
skip_mode = False

# Pending specialty continuation
pending_spec_continuation = None  # e.g., "TRAUMATOLOGÍA Y" waiting for "CIRUGÍA ORTOPÉDICA"

entry_name = None
entry_centro = None
entry_direccion = None
entry_telefono = None
entry_page = None

def flush():
    global entry_name, entry_centro, entry_direccion, entry_telefono, entry_page
    if entry_name and current_specialty:
        name = entry_name.strip()
        name = re.sub(r'\s+\d{1,2}$', '', name).strip()
        name = re.sub(r'\s*\((?:Suelo pélvico|Neuropsicología|solo ginecológica|Rehabilitación suelo pélvico)\)\s*$', '', name, flags=re.IGNORECASE).strip()

        if len(name) >= 5 and "," in name:
            municipio = current_municipio or current_zona or ""
            zona = current_zona or ""
            results.append({
                "aseguradora": "Adeslas",
                "especialidad_original": normalize_specialty(current_specialty),
                "profesional": name,
                "centro": entry_centro or "",
                "direccion": entry_direccion or "",
                "telefono": entry_telefono or "",
                "municipio": municipio,
                "zona": zona,
                "pagina_pdf": entry_page,
            })
    entry_name = None
    entry_centro = None
    entry_direccion = None
    entry_telefono = None
    entry_page = None

def process_other(text, page_num):
    """Process text that isn't a name, specialty, or municipio."""
    global entry_centro, entry_direccion, entry_telefono
    text = text.strip()
    if not text or len(text) < 2 or not entry_name:
        return
    if find_skip_at_start(text):
        return

    # Check for embedded phone even in consultation/schedule lines
    phone_match = re.search(r'(?:Teléfono|Teléfonos)\s+(\d[\d\s\-/]+)', text)
    if phone_match and not entry_telefono:
        phone = phone_match.group(1).strip()
        # Clean trailing non-phone content
        phone = re.sub(r'\s*(?:Consulta|previa|Horario).*', '', phone, flags=re.IGNORECASE).strip()
        if phone and len(phone) >= 9:
            entry_telefono = phone

    if is_consultation_note(text) or is_schedule_fragment(text):
        return
    if is_phone_line(text):
        phone = extract_phone(text)
        if phone:
            entry_telefono = (entry_telefono + " / " + phone) if entry_telefono else phone
        return
    if re.match(r'^\d{3}\s\d{2}\s\d{2}\s\d{2}$', text) or re.match(r'^\d{9}$', text):
        entry_telefono = (entry_telefono + " / " + text) if entry_telefono else text
        return
    if is_center(text):
        entry_centro = text
        return
    if not entry_direccion:
        if (re.search(r',\s*\d', text) or re.search(r's/n', text, re.IGNORECASE) or
            re.search(r'(?:bajo|izda|dcha|entresuelo|entlo|planta|local)\b', text, re.IGNORECASE) or
            re.match(r'^(?:Avda|Avda\.|C/|Plaza|Pza|Paseo|Pº|Ctra|Camino)\b', text, re.IGNORECASE) or
            re.match(r'^[A-ZÁÉÍÓÚÜÑÇ][a-záéíóúüñç].*,\s*\d', text)):
            entry_direccion = text

for page_num in sorted(pages.keys()):
    section = get_section(page_num)
    if section is None:
        continue
    page_lines = pages[page_num]
    if page_num in SUMMARY_PAGES or is_index_page(page_lines):
        continue

    if section == "Oviedo":
        current_zona = "Oviedo"
        current_municipio = "Oviedo"
    elif section == "Gijón":
        current_zona = "Gijón"
        current_municipio = "Gijón"
    elif section == "Municipios":
        current_zona = "Municipios"

    for line in page_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r'^\d{1,3}$', stripped):
            continue
        if '@' in stripped or 'www.' in stripped or 'https://' in stripped or stripped.startswith('%'):
            continue
        if stripped in ("Oviedo", "Gijón", "Municipios"):
            continue

        # --- Handle specialty continuations ---
        # "ESTOMATOLOGÍA" -> next line "(ODONTOLOGÍA)"
        # "TRAUMATOLOGÍA Y" -> next line "CIRUGÍA ORTOPÉDICA"
        # "CIRUGÍA GENERAL" -> sometimes next line "Y DEL APARATO DIGESTIVO"
        if pending_spec_continuation:
            if stripped.startswith("(ODONTOLOGÍA)") or stripped.startswith("(ODONTOLOGíA)"):
                current_specialty = "ESTOMATOLOGÍA (ODONTOLOGÍA)"
                pending_spec_continuation = None
                skip_mode = False
                continue
            elif stripped == "CIRUGÍA ORTOPÉDICA":
                current_specialty = "TRAUMATOLOGÍA Y CIRUGÍA ORTOPÉDICA"
                pending_spec_continuation = None
                skip_mode = False
                continue
            elif stripped.startswith("Y DEL APARATO DIGESTIVO") or stripped.startswith("Y DEL AP. DIGESTIVO"):
                current_specialty = "CIRUGÍA GENERAL Y DEL APARATO DIGESTIVO"
                pending_spec_continuation = None
                skip_mode = False
                continue
            else:
                # The continuation didn't happen, normalize what we had
                current_specialty = normalize_specialty(pending_spec_continuation)
                pending_spec_continuation = None
                skip_mode = False
                # Fall through to process this line normally

        # --- Municipality detection (Municipios section only) ---
        if section == "Municipios":
            muni, remainder = find_municipio_at_start(stripped)
            if muni:
                flush()
                mapped = MUNICIPIO_MAP.get(muni, muni.title())
                current_municipio = mapped
                # The remainder might be more municipality, skip section, or content
                if remainder:
                    # Check if remainder is another municipality
                    muni2, rem2 = find_municipio_at_start(remainder)
                    if muni2:
                        current_municipio = MUNICIPIO_MAP.get(muni2, muni2.title())
                    elif find_skip_at_start(remainder):
                        skip_mode = True
                        current_specialty = None
                    # Otherwise ignore remainder (usually address or skip content)
                continue

        # --- Skip section detection ---
        if find_skip_at_start(stripped):
            flush()
            skip_mode = True
            current_specialty = None
            continue

        # --- Find specialties and names ---
        specs = find_all_specs_in_text(stripped)
        names = find_names_in_text(stripped)

        # Build ordered list of items
        all_items = []
        for start, end, spec in specs:
            all_items.append((start, end, "SPEC", spec))
        for start, end, name in names:
            overlap = any(start < e and end > s for s, e, _, _ in all_items)
            if not overlap:
                all_items.append((start, end, "NAME", name))
        all_items.sort(key=lambda x: x[0])

        if all_items:
            last_end = 0
            for start, end, itype, ival in all_items:
                # Text between items
                if start > last_end:
                    between = stripped[last_end:start].strip()
                    if between:
                        process_other(between, page_num)

                if itype == "SPEC":
                    flush()
                    # Check if this spec needs continuation
                    if ival in ("ESTOMATOLOGÍA", "TRAUMATOLOGÍA Y"):
                        pending_spec_continuation = ival
                        current_specialty = ival  # Set tentatively
                    else:
                        current_specialty = ival
                        pending_spec_continuation = None
                    skip_mode = False
                elif itype == "NAME":
                    if skip_mode:
                        last_end = end
                        continue
                    if not current_specialty:
                        last_end = end
                        continue
                    flush()
                    entry_name = ival
                    entry_page = page_num

                last_end = end

            # Text after last item
            if last_end < len(stripped):
                after = stripped[last_end:].strip()
                if after:
                    process_other(after, page_num)
        else:
            # No names or specialties
            if skip_mode:
                continue
            if not current_specialty:
                continue
            process_other(stripped, page_num)

flush()

# --- Post-processing ---
cleaned = []
seen = set()
for r in results:
    name = r["profesional"]
    if any(kw in name.upper() for kw in ["POLICLÍNICA", "HOSPITAL", "CLÍNICA",
        "CENTRO MÉDICO", "SANATORIO", "LABORATORIO", "AMBULANCIAS", "TRANSINSA"]):
        continue
    if len(name) < 5 or "," not in name:
        continue

    # Clean phone
    if r["telefono"]:
        r["telefono"] = re.sub(r'(?:Consulta|previa|petición|Horario|de lunes|servicio).*',
                               '', r["telefono"], flags=re.IGNORECASE).strip().rstrip(" /.-")

    # Clean duplicated center names (from two-column merge)
    if r["centro"]:
        # Remove name patterns that leaked into center
        centro = r["centro"]
        # If center has a name pattern, extract just the center part
        name_in_centro = find_names_in_text(centro)
        if name_in_centro:
            # Keep only text before the first name
            first_name_start = name_in_centro[0][0]
            centro = centro[:first_name_start].strip()
        # Remove duplicated center names (e.g., "HOSPITAL BEGOÑA HOSPITAL BEGOÑA")
        for cn in CENTER_KEYWORDS:
            if centro.count(cn) > 1:
                centro = cn
                break
        # Remove phone info that leaked into center name
        centro = re.sub(r'\s*Teléfonos?\s+\d[\d\s\-/]+.*$', '', centro).strip()
        r["centro"] = centro

    # Clean addresses
    if r["direccion"]:
        dir_val = r["direccion"]
        # Remove phone info that leaked into address
        dir_val = re.sub(r'\s*Teléfonos?\s+\d[\d\s\-/]+.*$', '', dir_val).strip()
        # Remove parenthetical notes that aren't part of address
        dir_val = re.sub(r'\s*\((?:Rehabilitación[^)]*|Suelo pélvico|Neuropsicología|solo ginecológica|Psiquiátrica)\)', '', dir_val, flags=re.IGNORECASE).strip()
        # Remove consultation/schedule notes that leaked in
        dir_val = re.sub(r'\s*(?:Consulta|previa petición|Horario).*$', '', dir_val, flags=re.IGNORECASE).strip()
        # Simple dedup: if it contains the same address twice
        half = len(dir_val) // 2
        if half > 5 and dir_val[:half].strip() == dir_val[half:].strip():
            dir_val = dir_val[:half].strip()
        r["direccion"] = dir_val

    # Clean duplicated phones
    if r["telefono"]:
        # Normalize: split on / and -, extract individual phone numbers
        raw_phones = re.findall(r'\d{3}\s\d{2}\s\d{2}\s\d{2}', r["telefono"])
        if not raw_phones:
            raw_phones = re.findall(r'\d{9}', r["telefono"])
        seen_phones = []
        for p in raw_phones:
            p = p.strip()
            if p and p not in seen_phones:
                seen_phones.append(p)
        r["telefono"] = " / ".join(seen_phones)

    # Dedup
    key = (r["profesional"], r["especialidad_original"], str(r["pagina_pdf"]))
    if key in seen:
        continue
    seen.add(key)

    cleaned.append(r)

# --- Write CSV ---
fieldnames = ["aseguradora", "especialidad_original", "profesional", "centro",
              "direccion", "telefono", "municipio", "zona", "pagina_pdf"]

with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(cleaned)

print(f"Done! Wrote {len(cleaned)} rows to {OUTPUT}")

specialties = sorted(set(r["especialidad_original"] for r in cleaned))
print(f"\nSpecialties ({len(specialties)}):")
for s in specialties:
    count = sum(1 for r in cleaned if r["especialidad_original"] == s)
    print(f"  {s}: {count}")

municipios_out = sorted(set(r["municipio"] for r in cleaned))
print(f"\nMunicipios ({len(municipios_out)}):")
for m in municipios_out:
    count = sum(1 for r in cleaned if r["municipio"] == m)
    print(f"  {m}: {count}")
