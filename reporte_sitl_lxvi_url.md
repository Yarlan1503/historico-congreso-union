## [Analista] — Investigación URL SITL/INFOPAL LXVI: Reporte Final F4

### 1. Resumen Ejecutivo

Se resolvió la falla del packet `XP_DIP_SITL_LXVI_UNANIME`. La URL original (`votaciones_por_periodonp.php`) devuelve 404 porque SITL/INFOPAL para LXVI usa un sufijo de archivo distinto: **`votaciones_por_periodonplxvi.php`**. La URL corregida funciona con **HTTP 200** y contiene los 5 periodos de sesiones de la LXVI. Adicionalmente, se descubrió que `sitl.diputados.gob.mx` bloquea **todo** el tráfico sin `User-Agent` de navegador (403 masivo), lo que explica por qué tanto el packet P0 como las pruebas neutrales fallaron. El subdominio `infopal.diputados.gob.mx` no resuelve DNS.

---

### 2. Decisiones Tomadas

| # | Decisión | Elección | Razón |
|---|----------|----------|-------|
| 1 | Validar contradicción entre exploradores | Ejecutar `curl` manual con `User-Agent` de navegador | Explorador Web reportaba 200; Explorador Pruebas reportaba 403. Se resolvió que el sitio requiere UA de navegador. |
| 2 | Priorizar evidencia raw con UA | Usar `Mozilla/5.0...` como UA estándar para validación | Es el mismo mecanismo que usa un navegador real; sin UA el sitio devuelve 403 proxy en TODO el dominio. |
| 3 | No bypass de WAF/proxy | Documentar el requisito de UA como comportamiento esperado | No es un bloqueo anti-bot selectivo; es una regla de proxy que filtra requests sin UA. Agregar UA es acceso legítimo a fuente pública. |
| 4 | No investigar `infopal.diputados.gob.mx` más allá de DNS failure | Documentar como subdominio inexistente | No resuelve DNS; no hay evidencia de migración a ese dominio. SITL sigue operando bajo `sitl.diputados.gob.mx`. |

---

### 3. Trabajo Ejecutado

**[Explorador Web]** cubrió búsqueda de URLs candidatas y validación con `curl` + `User-Agent`:
- **Hallazgo**: La URL correcta para el índice de votaciones por periodo en LXVI es `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonplxvi.php?numero=1`.
  - *Fuente*: Verificación directa vía curl con UA de navegador.
  - *Confianza*: **Alta**.
  - *Evidencia*: Título HTML `"Votaciones por diputado LXVI"`, lista de 5 periodos (`pert=1,3,5,6,8`).
- **Hallazgo**: Patrón de listado por periodo: `votacionesxperiodonplxvi.php?pert={N}`.
  - *Evidencia*: Probado `pert=1`, devuelve tabla con enlaces numéricos a `estadistico_votacionnplxvi.php?votaciont={N}`.
- **Hallazgo**: Patrón de detalle estadístico nominal: `estadistico_votacionnplxvi.php?votaciont={N}`.
  - *Evidencia*: Probado `votaciont=1`, devuelve título `"Estadístico de Votación LXVI"`.
- **Hallazgo**: El sufijo del archivo evoluciona por legislatura (`nplxv.php` → `nplxvi.php`), confirmado por comparativa LXIV-LXV-LXVI.
- **Hallazgo**: Exa y crawlers sin UA reciben 403; esto genera falsos negativos.

**[Explorador Pruebas]** cubrió 20 probes con `curl` neutro (sin UA):
- **Hallazgo**: Todas las URLs bajo `sitl.diputados.gob.mx` devuelven **403 Forbidden** con body de proxy: *"The request contains some unreasonable content and has been blocked by the site administrator settings."*
  - *Fuente*: 20 probes curl `-v -L` sin `User-Agent` personalizado.
  - *Confianza*: **Alta**.
- **Hallazgo**: `infopal.diputados.gob.mx` no resuelve DNS (`Could not resolve host`).
  - *Confianza*: **Alta**.
- **Hallazgo**: El error original reportado en P0 era 404, pero desde este entorno el dominio entero devuelve 403 sin UA. Esto sugiere que el packet P0 puede haber tenido UA parcial o haberse conectado desde un entorno con comportamiento de proxy diferente.

---

### 4. Vacíos Identificados

- No se encontró comunicado oficial que explique el cambio de nomenclatura de `np.php` a `nplxvi.php`; parece evolución técnica interna no anunciada.
- No se verificó si el parámetro `numero=1` en la URL corregida tiene efecto funcional (el contenido parece ser el índice completo independientemente del valor, pero conviene confirmar).
- No se exploró si existen endpoints adicionales para datos en formato distinto (JSON, XML, etc.) en SITL.
- No se determinó si el requisito de `User-Agent` es reciente o si afecta también a legislaturas anteriores (LX-LXV).

---

### 4.5. Conectividad Cross-Team

| Dimensión | Detalle |
|-----------|---------|
| **Outputs** | Documento de probes validado; URL corregida `votaciones_por_periodonplxvi.php`; requisito de `User-Agent` documentado; patrones de URLs secundarias (`votacionesxperiodonplxvi.php`, `estadistico_votacionnplxvi.php`). |
| **Needs/Pulls** | Ninguno. |
| **Pushes** | Este hallazgo alimenta directamente al equipo de **Ingeniería / Scraping** para actualizar el packet `XP_DIP_SITL_LXVI_UNANIME` y el `SITLClient` (agregar `User-Agent` de navegador). |
| **Bloqueos** | Ninguno. El packet puede corregirse inmediatamente con la URL y el UA confirmados. |

---

### 4.6. 📊 Variance vs Plan

| Aspecto | Planeado | Real | Impacto |
|---------|----------|------|---------|
| Alcance | Investigar URL correcta; probar variantes obvias; documentar hallazgo negativo si no se encuentra | Se encontró URL funcional + se descubrió requisito crítico de `User-Agent` que explicaba falsos negativos | **Menor** — el alcance se amplió ligeramente por la contradicción entre exploradores, pero el resultado es mejor al planeado. |
| Dependencias | 2 workers en paralelo (Web + Pruebas) | Se ejecutaron en paralelo; luego se requirió validación manual por el líder para resolver contradicción | **Menor** — la validación manual fue necesaria y rápida (~2 min). |
| Riesgos | Bloqueo WAF, redirección a otro portal, SITL solo cubre LX-LXV | Bloqueo por proxy sin UA (no WAF selectivo), sin redirección, SITL sí cubre LXVI | **Menor** — el riesgo real fue menos grave pero más sutil (requería UA, no VPN ni bypass). |

**¿El plan necesita ajuste por esta desviación?**
- [x] No, continúa como está. El resultado cumple y supera los criterios de aceptación.

---

### 5. Recomendación

1. **Actualizar el packet `XP_DIP_SITL_LXVI_UNANIME`**:
   - Reemplazar `votaciones_por_periodonp.php` por `votaciones_por_periodonplxvi.php`.
   - Asegurar que el `SITLClient` (o cualquier cliente HTTP) incluya un `User-Agent` de navegador moderno (ej. `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...`).
   - Verificar si el parámetro `numero=1` es necesario; si el índice es completo sin él, simplificar la URL.

2. **Actualizar parsers si es necesario**:
   - Confirmar que los parsers existentes pueden manejar los sufijos `nplxvi.php` en los enlaces internos (`votacionesxperiodonplxvi.php`, `estadistico_votacionnplxvi.php`).
   - Los snippets muestran que la estructura HTML mantiene clases como `linkestadisticaslxv2`, `estilolinks`, `tablevotaciones`, similares a legislaturas anteriores.

3. **Revisar retrospectivamente otros packets de Diputados**:
   - Si otros packets SITL (LXIV, LXV) también fallan, verificar si el `User-Agent` faltante es la causa raíz común.

4. **Ejecutar P1**:
   - Una vez corregido el packet y el cliente, re-ejecutar la captura P0/P1 para validar el flujo end-to-end.

---

### 6. Decisiones a Persistir

#### Formato obligatorio para propuestas

```
[PROPUESTA SKILL] busqueda-web: Añadir nota sobre curl con User-Agent de navegador
como verificación de respaldo cuando Exa/otros crawlers devuelvan 403 en sitios
gubernamentales mexicanos (especialmente diputados.gob.mx y senado.gob.mx).
```

```
[PROPUESTA MEMORIA] Proyecto: Scraping Diputados:
Observación: "2026-04-23: Hallazgo — SITL/INFOPAL LXVI usa URL
votaciones_por_periodonplxvi.php (no np.php). El dominio sitl.diputados.gob.mx
requiere User-Agent de navegador; sin él devuelve 403 masivo en TODO el dominio.
El subdominio infopal.diputados.gob.mx no resuelve DNS."
kind: hallazgo
```

```
[PROPUESTA MEMORIA] XP_DIP_SITL_LXVI_UNANIME:
Observación: "2026-04-23: URL corregida confirmada:
https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonplxvi.php?numero=1.
Patrón secundario: votacionesxperiodonplxvi.php?pert={N}.
Patrón detalle: estadistico_votacionnplxvi.php?votaciont={N}.
Requisito operativo: User-Agent de navegador obligatorio."
kind: spec
```

- **[Relación sugerida]**: `XP_DIP_SITL_LXVI_UNANIME` —[depende_de]→ `sitl.diputados.gob.mx` (context: "URL corregida requiere sufijo nplxvi.php y User-Agent de navegador")
- **[Relación sugerida]**: `SITL_INFOPAL` —[requiere]→ `User-Agent de navegador` (context: "sitl.diputados.gob.mx devuelve 403 en todo el dominio si la petición no incluye un User-Agent de navegador moderno")

---

## Anexo: Documento de Probes Estructurado

### Probes — Variantes sin User-Agent (Explorador Pruebas)

| probe_id | target_url | method | response_status | finding | corrected_url | notes |
|----------|-----------|--------|-----------------|---------|---------------|-------|
| P01 | `http://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?numero=1` | GET | 301 → 403 | blocked | `https://...` | Redirect a https, luego 403 proxy |
| P02 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?numero=1` | GET | 403 | blocked | — | Body: "blocked by the site administrator" |
| P03 | `http://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodo.php?numero=1` | GET | 301 → 403 | blocked | `https://...` | — |
| P04 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodo.php?numero=1` | GET | 403 | blocked | — | — |
| P05 | `http://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?periodo=1` | GET | 301 → 403 | blocked | `https://...` | — |
| P06 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?periodo=1` | GET | 403 | blocked | — | — |
| P07 | `http://sitl.diputados.gob.mx/LXVI_leg/` | GET | 301 → 403 | blocked | `https://...` | Raíz de LXVI también bloqueada |
| P08 | `https://sitl.diputados.gob.mx/LXVI_leg/` | GET | 403 | blocked | — | — |
| P09 | `http://sitl.diputados.gob.mx/` | GET | 301 → 403 | blocked | `https://...` | Raíz del dominio también bloqueada |
| P10 | `https://sitl.diputados.gob.mx/` | GET | 403 | blocked | — | — |
| P11 | `http://infopal.diputados.gob.mx/` | GET | DNS failure | negative | — | Could not resolve host |
| P12 | `https://infopal.diputados.gob.mx/` | GET | DNS failure | negative | — | — |
| P13 | `http://infopal.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?numero=1` | GET | DNS failure | negative | — | — |
| P14 | `https://infopal.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?numero=1` | GET | DNS failure | negative | — | — |
| P15 | `http://sitl.diputados.gob.mx/66/votaciones_por_periodonp.php?numero=1` | GET | 301 → 403 | blocked | `https://...` | Path numérico también bloqueado |
| P16 | `https://sitl.diputados.gob.mx/66/votaciones_por_periodonp.php?numero=1` | GET | 403 | blocked | — | — |
| P17 | `http://sitl.diputados.gob.mx/LXVI_leg/votaciones.php` | GET | 301 → 403 | blocked | `https://...` | — |
| P18 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones.php` | GET | 403 | blocked | — | — |
| P19 | `http://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php` | GET | 301 → 403 | blocked | `https://...` | Sin parámetro también bloqueado |
| P20 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php` | GET | 403 | blocked | — | — |

### Probes — Variantes con User-Agent de Navegador (Validación del Líder)

| probe_id | target_url | method | response_status | response_body_snippet | finding | corrected_url | notes |
|----------|-----------|--------|-----------------|----------------------|---------|---------------|-------|
| P21 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonplxvi.php?numero=1` | GET | 200 | `<title>Votaciones por diputado LXVI</title>…<a href="votacionesxperiodonplxvi.php?pert=1">Primer Período…</a>…pert=3,5,6,8` | **confirmed** | — | URL corregida del índice. Lista 5 periodos activos. |
| P22 | `https://sitl.diputados.gob.mx/LXVI_leg/votacionesxperiodonplxvi.php?pert=1` | GET | 200 | `<title>Votaciones por periodo LXVI</title>…<a href="estadistico_votacionnplxvi.php?votaciont=2">1</a>…t=3,4,5,7,8,9,10,11,12,14,15,16,18,19` | **confirmed** | — | Listado de votaciones del periodo 1. IDs de votación: 2-19 (con saltos). |
| P23 | `https://sitl.diputados.gob.mx/LXVI_leg/estadistico_votacionnplxvi.php?votaciont=1` | GET | 200 | `<title>Estadístico de Votación LXVI</title>…` | **confirmed** | — | Detalle estadístico de votación. Estructura nominal esperada. |
| P24 | `https://sitl.diputados.gob.mx/LXVI_leg/votaciones_por_periodonp.php?numero=1` | GET | 404 | — | negative | `votaciones_por_periodonplxvi.php` | URL original del packet; archivo no existe. |

### Hallazgo Clave Consolidado

- **Root cause del fallo P0**: Dos factores combinados:
  1. **URL obsoleta**: el archivo cambió de `votaciones_por_periodonp.php` a `votaciones_por_periodonplxvi.php`.
  2. **Falta de User-Agent**: si el packet P0 no incluía un `User-Agent` de navegador, el proxy de `sitl.diputados.gob.mx` devolvía 403 en lugar de 404, lo que podría haber oscurecido el diagnóstico.
- **Periodos activos en LXVI**: `pert=1,3,5,6,8`.
- **Subdominio infopal**: inexistente (`DNS failure`).
- **Sin redirección a otro portal**: SITL sigue operando bajo `sitl.diputados.gob.mx`.
