# Task 3.5: resultado del STEP 1 (DETENTE 3.5.b)

Rama `docs/phase3-readme`. Decisiones de partida: `docs/internal/phase3_task3.5_step0_report.md`, sección 7. Trazabilidad: `docs/internal/phase3_task3.5_traceability.md`.

## Qué se hizo

- `README.md` y `docs/CASE_STUDY.md` (unas 1740 palabras), en inglés y en voz impersonal.
- `tests/unit/test_public_docs.py`, en `make test` (job `checks`): las cifras del README contra `bench.report.render()`, enlaces, anclas, rutas, el diagrama y el aviso de datos sintéticos.
- Backlog: cerrados los dos puntos de la Task 3.5 sobre las capturas (alturas de ventana, decisión 9) y el pie del fallo de desequilibrio.

## "How to run it", ejecutado (2026-10-13, Windows 11, Git Bash, Docker Desktop)

Partiendo de `make down` sin contenedores del proyecto; `.env` local con las contraseñas por defecto de ThingsBoard CE.

| Paso | Inicio (UTC) | Salida | Tiempo |
|---|---|---|---|
| `make down` | 22:34:19 | 0 | 1 s |
| `make install` (sobre un `.venv` existente) | 22:34:20 | 0 | 17 s |
| `make test` | 22:34:37 | 0 | 43 s (446 passed, 1 skipped en Windows; 119 Node) |
| `make up-pipeline` | 22:35:21 | 0 | 54 s |
| `make smoke-containers` | 22:36:14 | 0 | 127 s (PASS en 125,4 s) |
| simulador en contenedor, comando documentado, parado con `docker stop` a los 480 s | 22:38:21 | 0 | 2400 mensajes enviados y confirmados |
| simulador en el host sin fallos, `--duration 60` | 22:46:23 | 0 | 61 s |
| simulador en el host con los fallos documentados, `--duration 20` | 22:47:39 | 0 | 20 s |
| `make down` | 22:48:00 | 0 | 10 s |

Volúmenes de Docker: 93 antes y 93 después. Ningún paso necesitó algo no documentado.

**Lo que ve el operador.** A los 480 s del simulador con fallos, la API REST de ThingsBoard (la que alimenta el dashboard "Pipeline operations") devolvía exactamente las tres alarmas activas de la captura 01: pump-01 "Vibration RMS high (warning)", compressor-01 "Bearing temperature high (warning)" y "(critical)", todas `ACTIVE_UNACK`. Tras 60 s del simulador sin fallos y 15 s de espera: 0 alarmas activas, como en la captura 03. El dashboard no se miró en un navegador.

## Desvíos

- La captura 01 necesita dos fallos, no solo `pump-01:imbalance`: las alarmas de temperatura de compressor-01 vienen de `bearing_overheating`. El comando documentado lleva los dos, a los 300 s.
- Las dos ejecuciones en el host llevan `--duration` para terminar solas; el comando documentado no lo lleva. La de contenedor es el comando documentado literal.
- `87398cf` subió el primer borrador del README porque falló el script de edición; `44df693` aplicó las correcciones (sin estimación de memoria sin fuente, F1 como "read from Kafka", sin motivo inventado para la falta de p99 en F2).
- La primera versión del test de sincronía leía el encabezado de la tabla con el nivel del informe de cierre (`###`) y no con el de `make bench-report` (`##`); el test de deriva pasaba por el error equivocado. Corregido antes del commit: cada test de deriva comprueba primero que el par sin cambios pasa.
- El `make test` cronometrado no incluyó `test_public_docs.py`, que se escribió mientras corría; se ejecutó aparte (12 passed) y entra en CI.

## Sin verificar

- El dashboard en un navegador durante la verificación (se comprobó por la API REST).
- La insignia de CI: el repositorio es privado y la imagen no se sirve sin sesión.
- `make install` en un clon nuevo (se ejecutó sobre el `.venv` existente).
