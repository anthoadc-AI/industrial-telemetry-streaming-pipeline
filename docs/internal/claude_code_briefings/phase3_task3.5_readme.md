# Task 3.5 — README final y caso de estudio

## Contexto
main en c576f2b, CI verde en los cuatro jobs. Tasks 3.1 a 3.4 y la corrección
H3 cerradas. El README sigue siendo un stub de tres líneas. El repo es
privado; se hace público en la Task 3.6.

Esta tarea produce los dos documentos que leerá alguien de fuera antes que
cualquier otro:
- `README.md`: la primera impresión. Un reclutador lo lee en dos minutos; un
  entrevistador técnico, en veinte.
- `docs/CASE_STUDY.md`: el análisis de fondo que Anthony pidió: qué se hizo,
  por qué, cómo se aplica a una planta real y qué aporta la ingeniería de
  datos a la industria. Va aparte para que el README se mantenga sobrio y
  el análisis tenga espacio.

Las tres capturas del dashboard están en `docs/images/` (Anthony las añade
antes de lanzar este briefing).

## Reglas
- Rama `docs/phase3-readme` desde main. Sin merge hasta la aprobación.
- Camino C: los dos documentos en inglés; notas y reportes en `docs/internal/`
  en español.
- **Ninguna cifra sin fuente.** Cada número del README sale de
  `docs/internal/phase3_task3.3_closure_report.en.md` (que a su vez sale de
  `make bench-report`) o de un resultado de test citado por nombre. Las
  cifras de latencia se dan en milisegundos enteros, como las imprime el
  informe. Ninguna cifra se escribe de memoria ni se redondea a mano.
- Toda cifra lleva sus condiciones al lado: un portátil, Docker Desktop sobre
  WSL 2, un broker en tmpfs, datos sintéticos, ventanas de 300 s. Son las
  limitaciones de la sección 4 del informe de cierre.
- Formato Problema-Acción-Resultado para cada hallazgo, con sus números de
  antes y después.
- Cero emojis, cero insignias decorativas, cero adjetivos de venta
  ("robust", "production-grade", "cutting-edge" y similares quedan fuera).
  Las insignias de CI son aceptables solo si enlazan a un run real.
- El aviso de datos sintéticos aparece antes de cualquier cifra.
- Nada de información personal más allá del nombre de autor que ya usa el
  repo. Sin enlaces a perfiles.
- No se cambia código del producto ni `docs/ARCHITECTURE.md` salvo para
  corregir un enlace roto; cualquier otra cosa se reporta.

## STEP 0 — Inventario y esqueleto (sin redactar)
1. Estado: main en c576f2b, árbol limpio, rama creada, las tres capturas
   presentes en `docs/images/` con tamaño razonable (reporta los bytes).
2. Inventario de fuentes: lee los informes de cierre de las Fases 1 y 2 y de
   la Task 3.3, `phase3_fix_h3_result.md`, `docs/ARCHITECTURE.md` y el
   backlog. Lista cada cifra y cada hallazgo candidato con su fuente exacta
   (archivo y línea, o nombre del test).
3. Esqueleto del README, sección por sección, con una línea sobre qué
   responde cada una y de qué fuente sale. Estructura mínima:
   - qué es, en un párrafo, con el aviso de datos sintéticos;
   - arquitectura en un diagrama (reutiliza el de `ARCHITECTURE.md`) y cinco
     líneas, con enlace al documento completo;
   - cifras medidas: una tabla pequeña (latencia a 50 msg/s, alarma de
     extremo a extremo, throughput sostenido) con sus condiciones y enlace
     al informe del benchmark;
   - lo que el benchmark y la revisión encontraron y corrigieron: los cinco
     hallazgos (pérdida en la cola de Mosquitto, healthcheck JVM,
     StopTimeout de Docker Desktop, los dos defectos de reproceso de alarmas)
     en Problema-Acción-Resultado, tres líneas cada uno;
   - cómo correrlo: requisitos, `make up`, `make up-pipeline`, `make test`,
     `make smoke-containers`, verificados ejecutándolos;
   - capturas del dashboard con pie de foto que diga qué muestra cada una;
   - lógica de alarmas en tres líneas, con la referencia a la norma de
     vibración tal como está en `ARCHITECTURE.md`;
   - verificación: tests, cobertura y CI, con cifras del último run de main;
   - limitaciones conocidas, las principales, con enlace a la sección 13;
   - origen: el curso y el tag `v0-coursework`, en dos líneas;
   - "Why this matters", cinco a ocho líneas, con enlace al caso de estudio.
4. Esqueleto de `docs/CASE_STUDY.md`. Debe responder, en este orden y con
   los hechos del repo como base:
   - el problema de negocio: qué cuesta no detectar a tiempo la degradación
     de un activo rotativo, y qué datos existen hoy en una planta;
   - por qué un pipeline de streaming y no un informe por lotes: qué cambia
     cuando la alarma llega en segundos;
   - qué se construyó y qué decisiones de diseño responden a problemas
     reales de planta (orden por activo, persistencia e histéresis,
     entrega sin pérdida, reconciliación);
   - qué demostró el benchmark y qué no demuestra (una máquina, sintético);
   - cómo se aplicaría a una planta real: qué se reemplaza (simulador por
     sensores y gateways), qué se mantiene, qué falta (seguridad, alta
     disponibilidad, retención), con referencia a las limitaciones;
   - qué aporta la ingeniería de datos a la industria, en concreto: con
     ejemplos que se apoyen en lo que este repo hace, no en generalidades.
   Longitud objetivo: 1200 a 1800 palabras. Sin afirmaciones económicas o
   de mercado que no estén respaldadas; si se cita una fuente externa, se
   enlaza y se marca como externa.
5. Propuesta de comprobación automática: extender el test de sincronía para
   que las cifras del README coincidan con la salida de `make bench-report`
   y para que todo enlace interno del README y del caso de estudio exista.
6. Lista de decisiones que necesita Anthony.

### DETENTE 3.5.a
Detente. Reporta el inventario de cifras con fuentes, los dos esqueletos,
la propuesta de test y las decisiones pendientes. No redactes todavía.

## STEP 1 — Redacción
Solo tras la aprobación del DETENTE 3.5.a.
1. Escribe `README.md` y `docs/CASE_STUDY.md` según los esqueletos
   aprobados.
2. Ejecuta de verdad los comandos de "cómo correrlo" en una máquina limpia
   de contenedores del proyecto (`make down`, sin `-v`) y registra cuánto
   tarda cada uno. Si alguno falla o necesita un paso no documentado,
   detente y reporta.
3. Test de sincronía extendido, en el job de checks.
4. Tabla de trazabilidad del README en `docs/internal/` (español): cada
   cifra y cada afirmación con su fuente.
5. Unit tests, push, CI verde. Comprueba el renderizado del README en
   GitHub (imágenes y diagrama).

### DETENTE 3.5.b
Detente. Reporta: los dos documentos completos, los tiempos de los comandos
de "cómo correrlo", la tabla de trazabilidad, CI, la URL del README en la
rama, desvíos y lo que quedó sin verificar. Sin merge. El cierre lo aprueba
Anthony.
