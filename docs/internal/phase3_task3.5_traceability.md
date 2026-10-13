# Task 3.5: trazabilidad del README y del caso de estudio

Cada cifra y cada afirmación de `README.md` y `docs/CASE_STUDY.md` con su fuente. "Informe" es `docs/internal/phase3_task3.3_closure_report.en.md`; su sección 1 es la salida de `make bench-report`. "ARCH" es `docs/ARCHITECTURE.md`. Los números de línea son los de `main` en `c576f2b`.

Comprobación automática: `tests/unit/test_public_docs.py` compara la tabla de cifras y la frase de F3 con `bench.report.render()` sobre `bench/results/`, comprueba enlaces, anclas y rutas de los dos documentos, que el diagrama del README sea el de ARCH §2 y que el aviso de datos sintéticos vaya antes de cualquier cifra. Lo que el test no comprueba (las cifras de CI, la prosa) se apoya solo en esta tabla.

## README.md

| Sección | Afirmación o cifra | Fuente |
|---|---|---|
| Insignia | estado del workflow `CI` en main | `.github/workflows/ci.yml` (`name: CI`) |
| Aviso | todos los datos sintéticos; `synthetic: true` en cada registro | ARCH:3-5 |
| Qué es | cinco máquinas: dos motores, dos bombas, un compresor | ARCH:43 (`motor-01`, `motor-02`, `pump-01`, `pump-02`, `compressor-01`) |
| Qué es | cuatro canales; MQTT, Avro, persistencia e histéresis, ThingsBoard | ARCH:7, 44 |
| Arquitectura | diagrama | ARCH:55-82, literal (test) |
| Arquitectura | validación contra el esquema Avro, carta muerta intacta con motivo | ARCH:116-127 |
| Arquitectura | orden por máquina con clave por activo; Avro con Schema Registry como contrato | ARCH:87, 166-175, 608 |
| Arquitectura | umbrales solo en el alarm consumer; restauración desde `telemetry.alarms` | ARCH:206, 260-267, 604 |
| Arquitectura | ThingsBoard mapea, no decide | ARCH:299-303 |
| Condiciones | portátil, WSL 2, 8 vCPU, servicios fijados a vCPU | informe:151; `config/benchmark.yaml` (`cpusets`, "the VM has 8 CPUs") |
| Condiciones | un host, una red Docker; broker único, RF 1, tmpfs; ~218 B; ventanas de 300 s | informe:151, 157, 158 |
| Condiciones | mediana y rango de 5 corridas, ms enteros | informe:9, 17 |
| Tabla | F1 50 msg/s: 7 (7-8) / 11 (11-12) / 13 (12-13), n 15000, 5 corridas | informe:26 (test) |
| Tabla | F2 50 msg/s: 35 (34-36) / 55 (52-56) / -, n 300 | informe:29 (test) |
| Tabla | F1 2000 msg/s: 6 (6-6) / 11 (10-16) / 26 (19-59), n 600000 | informe:31 (test) |
| F3 | 2000 msg/s, 5 de 5; 3000 no medible válidamente, generador al límite; sin paso intermedio | informe:39-45, 54, 66 (test) |
| F3 | bridge a INFO, su nivel por defecto | informe:37, 154 |
| F1 | desde el instante programado: una parada del generador cuenta como latencia | informe:152 |
| F2 | patrón de fallo fijo; termina en el ack de Kafka, antes de ThingsBoard | informe:153; salida de `make bench-report` ("F2 alarm: the triggering sample's ts to the alarm consumer's acknowledgement") |
| ThingsBoard | 10 msg/s por activo por configuración; F5-F7 no medidas | ARCH:437, 448; informe:160 |
| Hallazgo 1 | 1040 mensajes por cliente, medido; 61,140 descartados a 3000 msg/s | informe:107, 110 |
| Hallazgo 1 | cola 60,000 / 25 MiB, monitor, dos tests | informe:111-113 |
| Hallazgo 1 | 1,040 de 5,000 antes, 5,000 de 5,000 después; ninguna caída en la serie final | informe:116, 118 |
| Hallazgo 2 | JVM cada ~11 s en las vCPU del broker | informe:122 |
| Hallazgo 2 | comparación en reposo de 300 s con el método del harness | informe:123 |
| Hallazgo 2 | 18,0 % a 5,9 % de un núcleo; muestras ≥ 50 %: 17 a 1 | informe:125 |
| Hallazgo 3 | 1 s del proxy frente a 10 s del motor; consumer 1,5-3,6 s, matado a medio parar | informe:83-87 |
| Hallazgo 3 | `stop_grace_period` explícito, 30 s en el pipeline | informe:91-92 |
| Hallazgo 3 | salidas 0 o 143, ninguna por SIGKILL; ThingsBoard ignora SIGTERM; test de salida limpia | informe:94-95, 100; ARCH:469 |
| Hallazgo 4 | CLEARED espurio tras restore; guarda de reproceso; test que falla antes y pasa después | ARCH:269, 282-284, 292-295; `docs/internal/phase3_fix_h3_result.md` |
| Hallazgo 5 | alarma fantasma, encontrada durante la corrección; mismo guarda; test | ARCH:286-288, 292-294; `docs/internal/phase3_fix_h3_result.md` |
| Cómo correrlo | Python 3.13.4, Node.js 24.16.0 | `.python-version`, `.nvmrc`; ARCH:484-485 |
| Cómo correrlo | Git Bash, PowerShell, bash | `Makefile` (comentario de cabecera) |
| Cómo correrlo | contraseñas por defecto de ThingsBoard CE | `.env.example` (comentarios de `TB_TENANT_PASSWORD`, `TB_SYSADMIN_PASSWORD`) |
| Cómo correrlo | qué hace cada target | `make help` (`Makefile`, `HELP`) |
| Cómo correrlo | `smoke-containers` necesita `telemetry.raw` vacío | `make help`; `scripts/smoke/run_smoke_containers.py` |
| Cómo correrlo | comandos y tiempos | ejecución del STEP 1, `docs/internal/phase3_task3.5_result.md` |
| Datos en vivo | `--fault ASSET:MODE:START_S`, perfil `simulator` | `edge/simulator/__main__.py`; `docker-compose.yml` (servicio `simulator`) |
| Datos en vivo | el fallo se mantiene hasta que el simulador para | `config/telemetry.yaml` ("step at once, ramp linearly over ramp_s, then held") |
| Datos en vivo | `http://localhost:8080`, `tenant@thingsboard.org`, "Pipeline operations" | `.env.example` (`TB_HTTP_PORT`, `TB_TENANT_USERNAME`); `processing/thingsboard/dashboards/operations.json` (`title`) |
| Capturas | fecha, contenido de cada una | Anthony (2026-10-13), comprobado mirando las imágenes |
| Capturas | escalón de 4 mm/s hasta ~6,5 mm/s; resto de canales con su deriva normal | `config/telemetry.yaml` (`imbalance.magnitude`, comentario de 6,5 mm/s); `docs/internal/phase3_backlog.md:62` |
| Lógica de alarmas | HI/HIHI al estilo ISA-18.2; persistencia e histéresis | ARCH:210-214, 252-256 |
| Lógica de alarmas | 4,5 y 7,1 mm/s, ISO 20816-3, Grupo 2, soportes flexibles; no derivados; temperatura y corriente ilustrativos | ARCH:234; enlace externo `https://www.iso.org/standard/78311.html` (página de ISO 20816-3:2022, consultada el 2026-10-13) |
| Lógica de alarmas | cada fallo cambia un canal | `config/telemetry.yaml` (`faults`) |
| Verificación | cuatro jobs; full solo en main | ARCH:558-565 |
| Verificación | run 38472910691, `c576f2b`, 2026-10-13, cuatro jobs en verde | `gh run view 38472910691` |
| Verificación | 447 Python, 119 Node | log del run: "447 passed", "Tests: 119 passed" |
| Verificación | core 16 passed, 16 skipped; full 32 passed | log del run |
| Verificación | "Total coverage: 98.54%", "845/874 branches, 96.68 %" | log del run, job "Lint and unit tests" |
| Verificación | Node: líneas 100 % (1159/1159), ramas 98,7 % (305/309) | log del run, resumen de Jest |
| Verificación | umbral del 80 % por lenguaje | ARCH:562; `pyproject.toml`, `jest.config.js` |
| Limitaciones | un host, RF 1 en tmpfs; sin seguridad; at most once antes del bridge; 10 msg/s; reloj | ARCH §13, puntos 1, 3, 4, 8, 11, 17 |
| Origen | curso MIT xPRO, Módulo 24; tag `v0-coursework` | ARCH:35 |
| Why this matters | señales que una planta ya recoge; el camino del sensor al operador | afirmación cualitativa, sin cifras |

## docs/CASE_STUDY.md

| Sección | Afirmación o cifra | Fuente |
|---|---|---|
| 1 | señales simuladas: temperatura de rodamiento, vibración, corriente, velocidad | ARCH:44 |
| 1 | fallos de máquinas rotativas visibles en señales medibles; coste de la detección tardía | afirmación cualitativa; sin cifras (decisión 10) |
| 2 | rampa de 45 °C en 180 s | `config/telemetry.yaml` (`bearing_overheating`) |
| 2 | WARNING y CRITICAL de compressor-01 a 58 s | captura 01: 17:23:35 y 17:24:33 |
| 2 | mediana de 35 ms a 50 msg/s, hasta el ack de Kafka | informe:29 |
| 2 | persistencia en muestras, una por segundo | ARCH:113, 218-221 |
| 3 | particionado por activo; 19 de 48 alarmas tras un reproceso con el particionador por defecto | ARCH:166-175; `docs/internal/phase3_backlog.md:12` |
| 3 | test de una partición por máquina | `test_alarms_of_an_asset_share_one_partition` |
| 3 | persistencia 3 (temperatura) y 5 (vibración, corriente) | ARCH:218-221 |
| 3 | Mosquitto descartaba y confirmaba | informe:105-118 |
| 3 | cola por defecto de ThingsBoard: 2 s, 3 reintentos; 43 de 48 alarmas | ARCH:613; `docs/internal/phase3_backlog.md:34, 112` |
| 3 | PipelineSequential: lote de 60 s, 20 reintentos; la sesión full falla si ThingsBoard descarta | ARCH:368, 587-594 |
| 3 | restauración, los dos defectos, guarda, tests | ARCH:260-297 |
| 3 | reconciliación en `make up` | ARCH:303 |
| 3 | esquemas, carta muerta, `alarm_id` determinista | ARCH:187-201, 127, 258 |
| 4 | 5 y 50 msg/s; 2000 msg/s 5 de 5; 3000 no medible | informe:21-45, 54 |
| 4 | un mensaje por máquina y segundo; 2000 msg/s = 2000 máquinas así | ARCH:113; aritmética, marcada como tal |
| 4 | límites de las cifras | informe:149-160 |
| 5 | lo que falta: seguridad, disponibilidad, una sola instancia del consumer, entrega antes del bridge, retención, umbrales, relojes | ARCH §13, puntos 1, 3, 4, 8, 17; ARCH:234 |
| 6 | Mosquitto, healthcheck, StopTimeout aparecieron solo al medir | informe §2 |
| 6 | test térmico que pasaba con cualquier constante | `docs/internal/phase2_closure_report.md:268` |
| 6 | defectos corregidos tras tests que fallan con el código anterior, demostrado en los dos sentidos | `docs/internal/phase3_fix_h3_result.md` |
| 6 | garantías por salto | ARCH:374-383 |
