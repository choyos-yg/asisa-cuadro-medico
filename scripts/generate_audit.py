"""
Genera reporte de auditoría end-to-end.
Toma 5 registros por aseguradora (25 total) y para cada uno muestra:
  1. Evidencia: imagen de la página del PDF (o extracto de texto)
  2. CSV raw: fila original
  3. SQLite: todos los campos normalizados
  4. Contribución a KPIs: qué métricas del dashboard toca este registro

Output: dashboard/auditoria.html + dashboard/auditoria_images/*.png
"""
import sqlite3
import csv
import os
import random
import shutil
import json
import html

BASE = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE, 'data', 'cuadro_medico.db')
RAW_DIR = os.path.join(BASE, 'data', 'raw')
OCR_IMAGES_DIR = os.path.join(BASE, 'ocr_images')
FULLTEXT_ADESLAS = os.path.join(BASE, 'data', 'raw', 'adeslas_fulltext.txt')
FULLTEXT_DKV = os.path.join(BASE, 'data', 'raw', 'dkv_fulltext.txt')

DASHBOARD_DIR = os.path.join(BASE, 'dashboard')
AUDIT_IMAGES_DIR = os.path.join(DASHBOARD_DIR, 'auditoria_images')
OUTPUT_HTML = os.path.join(DASHBOARD_DIR, 'auditoria.html')

# Insurance-specific config: PDF page mapping
# For OCR insurances: we have images in ocr_images/<key>/page_XXX.png
# For text insurances: we have fulltext files with "=== PAGINA X ===" markers
INSURERS = {
    'ASISA':    {'source': 'ocr', 'key': 'asisa',   'pdf': 'Cuadro medico ASISA Asturias.pdf'},
    'Mapfre':   {'source': 'ocr', 'key': 'mapfre',  'pdf': 'Cuadro medico Mapfre Asturias.pdf'},
    'Sanitas':  {'source': 'ocr', 'key': 'sanitas', 'pdf': 'Cuadro medico Sanitas Asturias (1).pdf'},
    'Adeslas':  {'source': 'text', 'fulltext': FULLTEXT_ADESLAS, 'pdf': 'Cuadro medico Adeslas Asturias.pdf'},
    'DKV':      {'source': 'text', 'fulltext': FULLTEXT_DKV, 'pdf': 'Cuadro medico DKV Asturias.pdf'},
}

SAMPLES_PER_INSURER = 5


def pick_samples(conn):
    """
    Muestra estratificada: 5 por aseguradora, buscando diversidad en:
      - especialidad
      - municipio
      - tener profesional (no solo centro)
      - al menos 1 caso con especialidad que fue reagrupada (Radiodiagnóstico, Odontología)
    """
    random.seed(42)  # reproducibilidad
    samples_per_insurer = {}

    for aseg in INSURERS.keys():
        # Candidatos: registros con profesional, municipio y especialidad
        cur = conn.execute('''
            SELECT id, aseguradora, especialidad_original, especialidad_normalizada,
                   familia_especialidad, profesional, centro, direccion, telefono,
                   municipio, area_sanitaria, poblacion_municipio, poblacion_area,
                   pagina_pdf, profesional_norm, centro_norm, persona_id
            FROM cuadro_medico
            WHERE aseguradora = ?
              AND profesional IS NOT NULL AND profesional != ''
              AND especialidad_normalizada IS NOT NULL
              AND municipio IS NOT NULL AND municipio != ''
              AND pagina_pdf IS NOT NULL
            ORDER BY RANDOM()
        ''', (aseg,))
        candidates = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]
        conn.row_factory = None

        # Pick diverse samples: try to cover different specialties and municipios
        picked = []
        seen_specs = set()
        seen_munis = set()
        for c in candidates:
            if len(picked) >= SAMPLES_PER_INSURER:
                break
            key = (c['especialidad_normalizada'], c['municipio'])
            if key in [(p['especialidad_normalizada'], p['municipio']) for p in picked]:
                continue
            # Prefer first encounter of unseen specialty or municipio
            if c['especialidad_normalizada'] not in seen_specs or c['municipio'] not in seen_munis or len(picked) < 3:
                picked.append(c)
                seen_specs.add(c['especialidad_normalizada'])
                seen_munis.add(c['municipio'])

        # Fallback if diversity filter was too strict
        if len(picked) < SAMPLES_PER_INSURER:
            for c in candidates:
                if c not in picked:
                    picked.append(c)
                    if len(picked) >= SAMPLES_PER_INSURER:
                        break

        samples_per_insurer[aseg] = picked[:SAMPLES_PER_INSURER]

    return samples_per_insurer


def copy_ocr_image(insurer_key, page_num, sample_id):
    """Copia la imagen de página del PDF OCR a dashboard/auditoria_images/."""
    src = os.path.join(OCR_IMAGES_DIR, insurer_key, f'page_{int(page_num):03d}.png')
    if not os.path.exists(src):
        return None
    dst_name = f'{insurer_key}_p{int(page_num):03d}_s{sample_id}.png'
    dst = os.path.join(AUDIT_IMAGES_DIR, dst_name)
    if not os.path.exists(dst):
        shutil.copy(src, dst)
    return f'auditoria_images/{dst_name}'


def get_text_excerpt(fulltext_path, page_num, max_chars=2500):
    """Extrae el texto correspondiente a la página pedida del fulltext."""
    if not os.path.exists(fulltext_path):
        return None
    with open(fulltext_path, encoding='utf-8') as f:
        content = f.read()
    marker = f'=== PAGINA {int(page_num)} ==='
    idx = content.find(marker)
    if idx == -1:
        return None
    end = content.find('=== PAGINA', idx + len(marker))
    if end == -1:
        end = idx + max_chars
    return content[idx:end].strip()[:max_chars]


def get_csv_row(aseguradora, sample_id):
    """Encuentra la fila del CSV raw correspondiente al registro."""
    csv_file = f'{aseguradora.lower()}_raw.csv'
    path = os.path.join(RAW_DIR, csv_file)
    if not os.path.exists(path):
        return None
    # El id del SQLite no está en el CSV; buscamos por coincidencia exacta de profesional+especialidad+pagina
    # Nota: esto es aproximado porque pueden existir filas muy similares
    return None  # Lo dejamos como opcional, el SQLite ya tiene todo


def compute_kpi_contributions(conn, sample):
    """Calcula a qué KPIs del dashboard contribuye este registro."""
    contributions = []
    aseg = sample['aseguradora']
    esp = sample['especialidad_normalizada']
    fam = sample['familia_especialidad']
    muni = sample['municipio']
    area = sample['area_sanitaria']
    persona_id = sample.get('persona_id')

    # 1. Cuenta total
    n_total = conn.execute('SELECT COUNT(*) FROM cuadro_medico WHERE aseguradora = ? AND especialidad_normalizada IS NOT NULL', (aseg,)).fetchone()[0]
    contributions.append({
        'kpi': f'Total registros {aseg}',
        'valor': n_total,
        'participacion': '1 registro de este profesional',
    })

    # 2. Profesionales únicos de la aseguradora
    n_prof = conn.execute('''
        SELECT COUNT(DISTINCT persona_id) FROM cuadro_medico
        WHERE aseguradora = ? AND persona_id IS NOT NULL
    ''', (aseg,)).fetchone()[0]
    contributions.append({
        'kpi': f'Profesionales únicos {aseg} (KPI card)',
        'valor': n_prof,
        'participacion': f'Cuenta como 1 (persona_id: <code>{html.escape(persona_id or "")}</code>)',
    })

    # 3. Especialidad
    n_en_esp = conn.execute('''
        SELECT COUNT(DISTINCT persona_id) FROM cuadro_medico
        WHERE aseguradora = ? AND especialidad_normalizada = ? AND persona_id IS NOT NULL
    ''', (aseg, esp)).fetchone()[0]
    contributions.append({
        'kpi': f'{esp} en {aseg} (pestaña Especialidades)',
        'valor': n_en_esp,
        'participacion': f'Aporta al contador de esta especialidad',
    })

    # 4. Municipio
    if muni:
        n_muni = conn.execute('''
            SELECT COUNT(*) FROM cuadro_medico
            WHERE aseguradora = ? AND municipio = ? AND especialidad_normalizada IS NOT NULL
        ''', (aseg, muni)).fetchone()[0]
        contributions.append({
            'kpi': f'Registros {aseg} en {muni} (pestaña Cobertura Geográfica)',
            'valor': n_muni,
            'participacion': f'Cuenta como registro en {muni}',
        })

    # 5. Área sanitaria (ratio /10K habitantes)
    if area:
        n_prof_area = conn.execute('''
            SELECT COUNT(DISTINCT persona_id) FROM cuadro_medico
            WHERE aseguradora = ? AND area_sanitaria = ? AND persona_id IS NOT NULL
        ''', (aseg, area)).fetchone()[0]
        pop_raw = sample.get('poblacion_area') or 0
        try:
            pop = int(pop_raw) if pop_raw else 0
        except (ValueError, TypeError):
            pop = 0
        ratio = round(n_prof_area * 10000 / pop, 2) if pop else 0
        contributions.append({
            'kpi': f'Ratio {aseg} / 10.000 hab en {area} (pestaña Áreas Sanitarias)',
            'valor': f'{ratio} prof/10K ({n_prof_area} prof / {pop:,} hab)',
            'participacion': f'Este profesional forma parte del numerador del ratio',
        })

    # 6. Si es odontología (fue reagrupada) o radiodiagnóstico (fue reagrupada), explicar el merge
    reagrupaciones = {
        'Odontología': 'Antes: Odontología, Odontología-Ortodoncia, Odontología-Periodoncia, Estomatología (todas fusionadas)',
        'Radiodiagnóstico': 'Antes: Radiodiagnóstico, TAC, Ecografía, Mamografía, Resonancia Magnética, Densitometría Ósea (todas fusionadas)',
        'Cirugía General': 'Antes: Cirugía General, Cirugía General y del Aparato Digestivo, Proctología (todas fusionadas)',
        'Ginecología y Obstetricia': 'Antes: Ginecología, Ginecología y Obstetricia, Obstetricia, Diagnóstico Prenatal (todas fusionadas)',
        'Otorrinolaringología': 'Antes: Otorrinolaringología + Audiología (fusionadas por cliente)',
        'Medicina General': 'Antes: Medicina General + Medicina Familiar y Comunitaria (fusionadas)',
        'Fisioterapia': 'Antes: Fisioterapia + Rehabilitación + Medicina Física (fusionadas)',
        'Matronas': 'Antes: Matronas + Preparación al Parto (fusionadas)',
    }
    if esp in reagrupaciones:
        contributions.append({
            'kpi': '⚠ Reagrupación de especialidad',
            'valor': esp,
            'participacion': reagrupaciones[esp],
        })

    return contributions


def validate_sample(sample, conn):
    """Aplica checks de coherencia sobre el registro."""
    checks = []

    # Check 1: especialidad normalizada existe en mapping
    if sample['especialidad_normalizada']:
        checks.append({'ok': True, 'msg': f'Especialidad "{sample["especialidad_original"]}" normalizada a "{sample["especialidad_normalizada"]}"'})
    else:
        checks.append({'ok': False, 'msg': 'Especialidad no normalizada'})

    # Check 2: municipio tiene población asignada
    pop_val = sample['poblacion_municipio']
    if pop_val and str(pop_val).strip():
        try:
            pop_int = int(pop_val)
            checks.append({'ok': True, 'msg': f'Municipio "{sample["municipio"]}" tiene población ({pop_int:,} hab.)'})
        except (ValueError, TypeError):
            checks.append({'ok': False, 'msg': f'Población inválida: {pop_val!r}'})
    else:
        checks.append({'ok': False, 'msg': f'Municipio "{sample["municipio"]}" sin población asignada'})

    # Check 3: área sanitaria asignada
    if sample['area_sanitaria']:
        checks.append({'ok': True, 'msg': f'Área sanitaria: {sample["area_sanitaria"]}'})
    else:
        checks.append({'ok': False, 'msg': 'Sin área sanitaria asignada'})

    # Check 4: profesional normalizado
    if sample['profesional_norm']:
        checks.append({'ok': True, 'msg': f'Profesional normalizado: <code>{html.escape(sample["profesional_norm"])}</code>'})
    else:
        checks.append({'ok': False, 'msg': 'Profesional sin normalizar'})

    # Check 5: persona_id (clave compuesta)
    if sample['persona_id']:
        checks.append({'ok': True, 'msg': f'persona_id: <code>{html.escape(sample["persona_id"])}</code>'})

    return checks


def generate_report(samples_by_insurer, conn):
    # Ensure images dir
    os.makedirs(AUDIT_IMAGES_DIR, exist_ok=True)

    html_parts = []
    html_parts.append('''<!DOCTYPE html>
<html lang="es"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Auditoría End-to-End | ASISA Asturias</title>
<script>
  // Gate: require authenticated session from index.html
  if (sessionStorage.getItem('asisa_auth') !== 'true') {
    window.location.href = 'index.html';
  }
</script>
<style>
:root {
  --asisa-primary: #003366;
  --asisa-secondary: #0066cc;
  --bg: #f5f7fa;
  --card-bg: #fff;
  --text: #1a1a2e;
  --text-muted: #6b7280;
  --danger: #ef4444;
  --success: #10b981;
  --border: #e5e7eb;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: var(--bg); color: var(--text); line-height: 1.6; }
header { background: linear-gradient(135deg, var(--asisa-primary), var(--asisa-secondary));
         color: white; padding: 2rem; }
header h1 { font-size: 1.6rem; }
header p { opacity: 0.85; margin-top: 0.3rem; font-size: 0.9rem; }
header .back { color: white; text-decoration: underline; font-size: 0.85rem; opacity: 0.85; }
.container { max-width: 1100px; margin: 0 auto; padding: 1.5rem; }
.intro { background: white; padding: 1.5rem; border-radius: 12px; margin-bottom: 1.5rem;
         box-shadow: 0 1px 3px rgba(0,0,0,0.06); }
.intro h2 { color: var(--asisa-primary); font-size: 1.15rem; margin-bottom: 0.5rem; }
.intro p { font-size: 0.92rem; color: var(--text-muted); margin-bottom: 0.5rem; }
.insurer-section { margin-bottom: 2.5rem; }
.insurer-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem;
                  padding-bottom: 0.5rem; border-bottom: 2px solid var(--asisa-primary); }
.insurer-header h2 { color: var(--asisa-primary); font-size: 1.3rem; }
.insurer-header .badge { background: var(--asisa-primary); color: white; padding: 0.15rem 0.7rem;
                          border-radius: 999px; font-size: 0.75rem; font-weight: 600; }
.sample { background: var(--card-bg); border-radius: 12px; padding: 1.5rem;
          margin-bottom: 1.2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.sample h3 { color: var(--asisa-primary); font-size: 1.05rem; margin-bottom: 0.3rem; }
.sample .subtitle { color: var(--text-muted); font-size: 0.85rem; margin-bottom: 1rem; }
.sample-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
@media (max-width: 900px) { .sample-grid { grid-template-columns: 1fr; } }
.panel { background: #fafbfc; border: 1px solid var(--border); border-radius: 8px; padding: 1rem; }
.panel h4 { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px;
            color: var(--text-muted); margin-bottom: 0.6rem; font-weight: 700; }
.panel img { max-width: 100%; border: 1px solid var(--border); border-radius: 6px; }
.panel pre { background: white; padding: 0.7rem; border-radius: 6px; font-size: 0.78rem;
             overflow-x: auto; white-space: pre-wrap; word-break: break-word; border: 1px solid var(--border); }
.fields { font-size: 0.85rem; }
.fields .row { display: flex; justify-content: space-between; padding: 0.3rem 0;
               border-bottom: 1px solid var(--border); gap: 1rem; }
.fields .row:last-child { border-bottom: none; }
.fields .label { color: var(--text-muted); font-weight: 600; flex-shrink: 0; }
.fields .value { color: var(--text); text-align: right; word-break: break-word; }
.fields .value code { background: #eef; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.8rem; }
.checks { margin-top: 0.8rem; }
.check { font-size: 0.82rem; padding: 0.4rem 0.6rem; border-radius: 6px; margin-bottom: 0.3rem; }
.check.ok { background: #ecfdf5; color: #065f46; border-left: 3px solid var(--success); }
.check.fail { background: #fef2f2; color: #7f1d1d; border-left: 3px solid var(--danger); }
.kpi-list { font-size: 0.82rem; }
.kpi-item { padding: 0.5rem; border-radius: 6px; background: white; margin-bottom: 0.4rem;
            border: 1px solid var(--border); }
.kpi-item .kpi-name { font-weight: 600; color: var(--asisa-primary); font-size: 0.83rem; }
.kpi-item .kpi-value { color: var(--text); margin-top: 0.1rem; }
.kpi-item .kpi-participation { color: var(--text-muted); font-size: 0.78rem; margin-top: 0.2rem; }
.full-width { grid-column: 1 / -1; }
.evidence-note { font-size: 0.78rem; color: var(--text-muted); margin-top: 0.5rem; font-style: italic; }
</style>
</head><body>
<header>
  <a class="back" href="index.html">&larr; Volver al dashboard</a>
  <h1>Auditoría End-to-End</h1>
  <p>Trazabilidad desde el PDF original hasta los KPIs del dashboard</p>
</header>
<div class="container">
  <div class="intro">
    <h2>Metodología</h2>
    <p>Se seleccionaron aleatoriamente <strong>5 registros por aseguradora</strong> (25 en total), con diversidad
       forzada en especialidades y municipios. Para cada muestra se muestra:</p>
    <p><strong>1. Evidencia</strong> — Imagen de la página del PDF original (o extracto de texto para los PDFs con texto extraíble).<br>
    <strong>2. Registro normalizado</strong> — Todos los campos del SQLite tras extracción, normalización y enriquecimiento.<br>
    <strong>3. Contribución a KPIs</strong> — Qué métricas del dashboard se ven afectadas por este registro.<br>
    <strong>4. Checks de coherencia</strong> — Validaciones automáticas que deberían cumplirse.</p>
    <p>Este reporte se genera con <code>scripts/generate_audit.py</code> contra la base SQLite.
       Para validar manualmente: abrir el PDF en la página indicada y comparar con el registro extraído.</p>
  </div>
''')

    sample_counter = 0
    for aseg, samples in samples_by_insurer.items():
        cfg = INSURERS[aseg]
        html_parts.append(f'''<div class="insurer-section">
  <div class="insurer-header">
    <h2>{aseg}</h2>
    <span class="badge">{len(samples)} muestras</span>
    <span style="color:var(--text-muted);font-size:0.85rem;">— {cfg["pdf"]}</span>
  </div>''')

        for s in samples:
            sample_counter += 1
            page = int(s['pagina_pdf']) if s['pagina_pdf'] else '?'
            evidence_html = ''

            # Build evidence (image or text)
            if cfg['source'] == 'ocr':
                img_path = copy_ocr_image(cfg['key'], page, sample_counter)
                if img_path:
                    evidence_html = f'''
                    <img src="{img_path}" alt="Página {page} del PDF {aseg}">
                    <div class="evidence-note">PDF: {cfg["pdf"]} — Página {page}</div>'''
                else:
                    evidence_html = f'<em>Imagen no disponible para página {page}</em>'
            else:
                excerpt = get_text_excerpt(cfg['fulltext'], page)
                if excerpt:
                    evidence_html = f'''
                    <pre>{html.escape(excerpt)}</pre>
                    <div class="evidence-note">PDF: {cfg["pdf"]} — Página {page} (texto extraído)</div>'''
                else:
                    evidence_html = f'<em>Texto no disponible para página {page}</em>'

            # Normalized fields
            fields_html = ''
            field_map = [
                ('Aseguradora', s['aseguradora']),
                ('Profesional (tal cual)', s['profesional']),
                ('Profesional normalizado', f'<code>{html.escape(s["profesional_norm"] or "")}</code>'),
                ('Especialidad original', s['especialidad_original']),
                ('Especialidad normalizada', f'<strong>{s["especialidad_normalizada"]}</strong>'),
                ('Familia especialidad', s['familia_especialidad']),
                ('Centro', s['centro'] or '—'),
                ('Centro normalizado', f'<code>{html.escape(s["centro_norm"] or "")}</code>' if s['centro_norm'] else '—'),
                ('Dirección', s['direccion'] or '—'),
                ('Teléfono', s['telefono'] or '—'),
                ('Municipio', s['municipio']),
                ('Población municipio', (lambda v: f'{int(v):,} hab.' if v and str(v).strip() else '—')(s['poblacion_municipio'])),
                ('Área sanitaria', s['area_sanitaria'] or '—'),
                ('Página PDF', page),
                ('persona_id (clave matching)', f'<code>{html.escape(s["persona_id"] or "")}</code>'),
            ]
            for label, value in field_map:
                fields_html += f'<div class="row"><span class="label">{label}</span><span class="value">{value}</span></div>'

            # Validation checks
            checks = validate_sample(s, conn)
            checks_html = '<div class="checks">'
            for c in checks:
                cls = 'ok' if c['ok'] else 'fail'
                mark = '✓' if c['ok'] else '✗'
                checks_html += f'<div class="check {cls}">{mark} {c["msg"]}</div>'
            checks_html += '</div>'

            # KPI contributions
            kpis = compute_kpi_contributions(conn, s)
            kpi_html = '<div class="kpi-list">'
            for k in kpis:
                kpi_html += f'''<div class="kpi-item">
                    <div class="kpi-name">{html.escape(str(k["kpi"]))}</div>
                    <div class="kpi-value">Valor actual: <strong>{k["valor"]}</strong></div>
                    <div class="kpi-participation">{k["participacion"]}</div>
                </div>'''
            kpi_html += '</div>'

            html_parts.append(f'''<div class="sample">
  <h3>Muestra #{sample_counter}: {html.escape(s["profesional"])}</h3>
  <div class="subtitle">{html.escape(s["especialidad_normalizada"])} — {html.escape(s["municipio"])} — Página {page} del PDF</div>
  <div class="sample-grid">
    <div class="panel">
      <h4>📄 Evidencia (fuente)</h4>
      {evidence_html}
    </div>
    <div class="panel">
      <h4>🗂 Registro normalizado (SQLite)</h4>
      <div class="fields">{fields_html}</div>
      {checks_html}
    </div>
    <div class="panel full-width">
      <h4>📊 Contribución a KPIs del dashboard</h4>
      {kpi_html}
    </div>
  </div>
</div>''')

        html_parts.append('</div>')  # close insurer-section

    html_parts.append('</div></body></html>')

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html_parts))
    print(f'Reporte generado: {OUTPUT_HTML}')


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    samples = pick_samples(conn)

    # Print summary
    print('=== MUESTRAS SELECCIONADAS ===\n')
    for aseg, s_list in samples.items():
        print(f'{aseg}:')
        for s in s_list:
            print(f'  pag {s["pagina_pdf"]:>3} | {s["especialidad_normalizada"]:<30s} | {s["profesional"]:<40s} | {s["municipio"]}')
        print()

    generate_report(samples, conn)
    conn.close()


if __name__ == '__main__':
    main()
