# Plan de Trabajo: Comparador de Cuadros Médicos - ASISA Asturias

**Cliente:** ASISA  
**Fecha inicio:** 2026-04-08  
**Última actualización:** 2026-04-09  
**Ámbito geográfico:** Asturias (Principado)  
**Aseguradoras analizadas:** ASISA, Adeslas, DKV, Mapfre, Sanitas

## ESTADO ACTUAL

| Fase | Estado | Detalle |
|------|--------|---------|
| Fase 1: Extracción | ✅ Completada | 3,231 registros de 5 PDFs |
| Fase 2: Normalización | ✅ Completada | 214 variantes → 62 especialidades |
| Fase 3: Enriquecimiento | ✅ Parcial | Población INE 2024 + 8 áreas SESPA incorporadas. Falta: datos de asegurados ASISA por zona |
| Fase 4: Análisis | ✅ Completada | KPIs competitivos, gaps, benchmark, ratios por 10K hab |
| Fase 5: Dashboard | ✅ v1 operativa | 7 pestañas, listo para GitHub Pages |

### Pendiente del cliente
- **Datos de asegurados ASISA por zona/municipio** → necesario para ratios por 10.000 asegurados (actualmente usamos población general INE)
- **Validar hallazgos** antes de presentar

### Hallazgos clave
- ASISA cubre **80.6%** de las especialidades del mercado (50/62)
- **12 especialidades** que la competencia ofrece y ASISA no
- **27 municipios** sin presencia ASISA donde sí hay competencia
- **45 gaps críticos** (zona+especialidad sin cubrir, cubierta por 2+ competidores)
- Área II (Cangas del Narcea): ratio más bajo (0.36 prof/10K hab vs 1.5 competencia)
- Área IV (Oviedo): ratio más alto (8.32 prof/10K)

---

## 1. Estado de los datos fuente

| PDF | Páginas | Texto extraíble | Método necesario | Estructura detectada |
|-----|---------|-----------------|------------------|----------------------|
| ASISA | 44 | No (escaneado) | OCR | Por determinar |
| Adeslas | 120 | Sí | pdfplumber | Ciudad → Especialidad → Médico (nombre, centro, dirección, teléfono) |
| DKV | 85 | Sí | pdfplumber | Ciudad → Especialidad → Médico (nombre, centro, dirección, teléfono) |
| Mapfre | 83 | No (escaneado) | OCR | Por determinar |
| Sanitas | 36 | No (escaneado) | OCR | Por determinar |

### Hallazgo crítico
3 de 5 PDFs son escaneados (imágenes). Se necesita OCR para ASISA, Mapfre y Sanitas.

**Opciones OCR:**
- Tesseract (local, gratuito)
- Claude Vision (enviar páginas como imágenes)
- Azure Document Intelligence / Google Document AI (APIs de pago)

**Recomendación:** Usar Claude Vision para OCR, ya que puede interpretar layouts complejos y tablas mejor que Tesseract en documentos médicos españoles.

---

## 2. Fases del proyecto

### FASE 1: Extracción de datos (ETL)
**Objetivo:** Obtener datos crudos de los 5 PDFs en formato estructurado.

- [ ] **1.1** Extraer Adeslas (pdfplumber) → CSV/JSON
- [ ] **1.2** Extraer DKV (pdfplumber) → CSV/JSON
- [ ] **1.3** Extraer ASISA (OCR) → CSV/JSON
- [ ] **1.4** Extraer Mapfre (OCR) → CSV/JSON
- [ ] **1.5** Extraer Sanitas (OCR) → CSV/JSON

**Esquema de datos objetivo por registro:**
```
{
  "aseguradora": "ASISA|Adeslas|DKV|Mapfre|Sanitas",
  "especialidad_original": "texto tal cual aparece en el PDF",
  "especialidad_normalizada": "categoría unificada",
  "profesional": "Nombre completo",
  "centro": "Nombre del centro/clínica",
  "direccion": "Dirección completa",
  "telefono": "Teléfono(s)",
  "municipio": "Municipio",
  "zona": "Oviedo|Gijón|Avilés|Resto Asturias",
  "tipo": "hospital|clinica|consulta_privada|centro_medico"
}
```

### FASE 2: Normalización de especialidades ✅ COMPLETADA
**Objetivo:** Unificar nomenclatura entre aseguradoras.
**Resultado:** 214 variantes → 62 especialidades normalizadas. 0 sin mapear.

#### Normalizaciones automáticas (alta confianza):
Estas equivalencias son estándar y no requieren validación del cliente:

| Variantes encontradas | Especialidad normalizada |
|----------------------|--------------------------|
| Fisioterapia / Rehabilitación | Fisioterapia y Rehabilitación |
| Aparato Digestivo / Gastroenterología | Aparato Digestivo |
| Traumatología y Cirugía Ortopédica / Traumatología y Ortopedia | Traumatología y Ortopedia |
| Angiología y Cirugía Vascular / Cirugía Vascular | Angiología y Cirugía Vascular |
| Endocrinología y Nutrición / Endocrino y Nutrición | Endocrinología y Nutrición |
| Dermatología y Venereología / Dermatología | Dermatología |
| Radiodiagnóstico / Diagnóstico por imagen | Radiodiagnóstico |
| Estomatología (Odontología) / Odontología | Odontología |
| Dietética y Nutrición / Nutrición | Dietética y Nutrición |

#### Normalizaciones que requieren validación del cliente:
Estas tienen ambigüedad o solapamiento parcial:

| Caso | Pregunta para el cliente |
|------|--------------------------|
| Medicina General vs. Medicina de Familia | ¿Son la misma categoría o se distinguen? |
| Psicología vs. Neuropsicología | ¿Neuropsicología es subcategoría de Psicología o independiente? |
| Cirugía General y del Ap. Digestivo vs. Cirugía General | ¿Se unifican o se mantienen separadas? |
| Preparación al Parto vs. Matronas | ¿Se agrupan bajo Ginecología o son independientes? |
| Audiología vs. Otorrinolaringología | ¿Audiología se agrupa con ORL o es independiente? |
| Logopedia vs. Foniatría | ¿Son equivalentes? |
| Podología | ¿Se considera especialidad médica o paraméd.? ¿Incluir en análisis? |

> **ACCIÓN:** Enviar esta tabla al cliente para que dirima antes de continuar con Fase 3.

### FASE 3: Enriquecimiento con datos de contexto
**Objetivo:** Añadir datos necesarios para calcular KPIs.

**Datos que necesitamos del cliente (ASISA):**
- [ ] **3.1** Número de asegurados por zona/municipio en Asturias
- [ ] **3.2** Definición de zonas geográficas (¿municipios? ¿comarcas? ¿áreas sanitarias?)
- [ ] **3.3** Coordenadas o códigos postales para cálculo de distancias (o confirmar si usamos geocodificación automática desde direcciones)

**Datos públicos a incorporar:**
- [ ] **3.4** Población por municipio (INE)
- [ ] **3.5** Mapa de áreas sanitarias de Asturias (SESPA)
- [ ] **3.6** Coordenadas geográficas de centros (geocodificación)

### FASE 4: Análisis y KPIs
**Objetivo:** Generar los indicadores solicitados.

#### 4A. Red médica interna (ASISA)
- [ ] Médicos por 10.000 asegurados (por especialidad y zona)
- [ ] Centros por 10.000 asegurados
- [ ] Nº total de profesionales por especialidad
- [ ] Nº de especialidades disponibles por zona
- [ ] % asegurados con acceso a cada especialidad
- [ ] Índice de cobertura geográfica (médicos en radio X km)
- [ ] Distancia media a centros/especialistas

#### 4B. Detección de gaps estructurales
- [ ] Especialidades con baja ratio médicos/asegurados
- [ ] Zonas con baja cobertura geográfica
- [ ] Zonas con pocas especialidades disponibles
- [ ] Dependencia de centros concretos (top 3 concentran % alto)
- [ ] Ranking de zonas mejor/peor cubiertas

#### 4C. Benchmark competitivo (ASISA vs mercado)
- [ ] Especialidades que tienen otros y ASISA no
- [ ] Diferencia de médicos por especialidad (ASISA vs competencia)
- [ ] Centros/hospitales donde ASISA no tiene presencia y otros sí
- [ ] Profesionales presentes en competencia y no en ASISA
- [ ] Zonas donde competencia tiene mayor densidad médica
- [ ] Nº total de profesionales y centros vs competencia

#### 4D. KPIs competitivos
- [ ] % de especialidades cubiertas vs competencia
- [ ] Ratio de densidad (ASISA / competencia)
- [ ] Nº de gaps críticos (zonas o especialidades por debajo de mercado)

### FASE 5: Outputs y entregables
- [ ] **5.1** Dashboard interactivo (WeWeb) o informe estático (PDF/Excel)
- [ ] **5.2** Ranking de zonas con mayor déficit
- [ ] **5.3** Ranking de especialidades más tensionadas
- [ ] **5.4** Informe de gaps: "Faltan X profesionales para igualar mercado"
- [ ] **5.5** Mapa de cobertura ASISA vs mercado (%) por zona
- [ ] **5.6** Lista de centros presentes en competencia pero no en ASISA
- [ ] **5.7** Recomendaciones priorizadas de expansión

---

## 3. Dependencias y bloqueos

| Bloqueante | Fase afectada | Acción |
|-----------|---------------|--------|
| ~~OCR de 3 PDFs (ASISA, Mapfre, Sanitas)~~ | Fase 1 | **RESUELTO:** Claude Vision via pypdfium2 → imagen → lectura |
| Validación de normalización de especialidades | Fase 2 → 3 | Enviar tabla al cliente |
| Datos de asegurados por zona | Fase 3 → 4 | Solicitar a ASISA |
| Definición de zonas geográficas | Fase 3 → 4 | Acordar con cliente |
| Formato de entregable final | Fase 5 | **DEFINIDO:** Dashboard HTML/JS en GitHub Pages |

---

## 4. Almacenamiento de datos

**Base de trabajo:** SQLite (`data/cuadro_medico.db`) — archivo local, sin servidor.  
**Exports:** CSVs para revisión en Excel.

```
Cuadro Médico/
├── PLAN_CUADRO_MEDICO.md
├── data/
│   ├── cuadro_medico.db              ← SQLite maestro (queries, KPIs)
│   ├── raw/                           ← CSVs crudos por aseguradora
│   ├── normalized/                    ← CSVs post-normalización
│   └── specialty_mapping.csv          ← Equivalencias de especialidades
├── scripts/                           ← Scripts Python de extracción
├── dashboard/                         ← HTML/JS para GitHub Pages
│   ├── index.html
│   ├── data.json                      ← Datos para el dashboard
│   └── assets/
```

---

## 5. OCR: Decisión y proyección de tokens

**Método elegido:** Claude Vision (pypdfium2 renderiza PDFs → imagen → lectura con Claude).  
**Tesseract:** No disponible en el sistema y no necesario — Claude Vision lee perfectamente estos PDFs.

### Proyección de tokens para OCR

| PDF | Páginas | Input tokens (low-res) | Input tokens (high-res) |
|-----|---------|----------------------|------------------------|
| ASISA | 44 | 66,000 | 154,000 |
| Mapfre | 83 | 124,500 | 290,500 |
| Sanitas | 36 | 54,000 | 126,000 |
| **Total** | **163** | **244,500** | **570,500** |

Output estimado: ~65,200 tokens (texto extraído).

| Escenario | Total tokens | % del contexto 1M |
|-----------|-------------|-------------------|
| Low-res | ~310K | 31% |
| High-res | ~636K | 64% |

**Estrategia:** Batches de 5 páginas por request = 33 requests totales.  
Con el límite diario da de sobra.

---

## 6. Entregable final

**Dashboard interactivo** en HTML/JS estático, desplegado en **GitHub Pages**.
- Desarrollo local primero
- Push a GitHub → GitHub Pages automático
- El cliente accede por URL sin instalar nada

---

## 7. Próximos pasos inmediatos

1. ~~Decidir método OCR~~ → **RESUELTO: Claude Vision**
2. **Comenzar extracción** de Adeslas y DKV (pdfplumber, ya viable)
3. **Extraer con OCR** ASISA, Mapfre y Sanitas (Claude Vision)
4. **Enviar al cliente** la tabla de normalización de especialidades
5. **Solicitar al cliente** datos de asegurados por zona
6. **Definir zonas geográficas** de análisis
