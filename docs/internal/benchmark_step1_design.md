# Task 3.3 STEP 1 — Verificaciones previas (E3) y diseño

Estado: **aprobado en el DETENTE 3.3.b con los cambios de la sección "Decisiones del DETENTE 3.3.b", y en el DETENTE 3.3.d con los de "Decisiones del DETENTE 3.3.d"** (al final). Prevalecen sobre el resto del documento donde difieran, y las del 3.3.d sobre las del 3.3.b. Fuente: `docs/internal/claude_code_briefings/phase3_task3.3_step1_benchmark_implementation.md`, secciones 1.0 y 1.1. Las enmiendas del 2026-10-09 están en `docs/internal/benchmark_protocol_proposal.md`.

## 1.0 Estado

- Rama `feature/phase3-benchmark` en `9ace303` (cuatro commits sobre `main`, `a895bb3`), árbol limpio, 0 contenedores corriendo, corriente alterna (`PowerLineStatus` Online, `Win32_Battery.BatteryStatus` = 2).
- **Desvío:** el briefing dice `78673d2` y tres commits. El commit del propio briefing (`9ace303`) es el cuarto, y coincide con el estado que indicó Anthony.
- E4 aplicada y sección "Enmiendas del 2026-10-09" añadida a la propuesta: commit `0d7e819`. Para redactar E4 comprobé, solo listando, que no queda ningún contenedor de Airflow ni de Hadoop y que sus imágenes y sus volúmenes siguen ahí: `airflow-docker`, `airflow_assignment`, `airflow` y `docker-hadoop` (`docker volume ls`).
- Cifras de tests del briefing, comprobadas:
  - Python: 292 superados y 1 omitido (`test_clean.py:63`, porque esta cuenta no puede crear enlaces simbólicos), es decir, **293**;
  - Jest: **108** superados (`make test`);
  - integración: **25** recolectados, de los que **12** corren en perfil core (los 13 de ThingsBoard se omiten) y **25** en full (`pytest tests/integration --collect-only`). En Windows, `test_signals` se omite siempre porque es solo POSIX.
  
  Las suites de integración no se ejecutaron en este paso.

## 1.1.1 Activos sintéticos

**Lectura del código:**

| Componente | Qué hace con un `asset_id` que no está en `config/telemetry.yaml` | Evidencia |
|---|---|---|
| Bridge | Lo acepta. Solo exige que el `asset_id` coincida con el del topic MQTT. El esquema define `asset_id` y `asset_type` como `string` libre. | `bridge/mqtt_to_kafka/validation.py:99-101`; `schemas/telemetry_v1.avsc` |
| Alarm consumer | Lo procesa sin reglas: `AlarmEngine.process` devuelve `[]` para un activo sin reglas, suma `messages_without_rules` y no lo manda a la DLQ. Las reglas salen solo de los activos configurados. | `processing/alarm_consumer/state_machine.js:120-126, 152-154`; `consumer.js:305` |
| Sink ThingsBoard (no entra en la prueba) | La telemetría usa el `asset_type` del propio registro. Las alarmas usan el tipo de la config, que sería `None` para un activo desconocido. | `processing/thingsboard_sink/__main__.py:141, 274, 354` |

**Prueba mínima** (`scripts/bench/diagnostics/synthetic_asset_check.py`), con el stack core y el bridge y el consumer reales en el host. Publica 4 muestras de `synthetic-0001` (tipo pump) y 4 de `pump-01` (el control), intercaladas y todas a 100 °C, por encima de los umbrales de warning (80) y critical (95), con persistencia de 3 muestras:

| Resultado | Valor |
|---|---|
| Bridge, `run_finished` | `messages_received` 8, `messages_delivered` 8, `messages_invalid` 0; 0 cartas muertas |
| Consumer, `run_finished` | `messages_processed` 8, `messages_without_rules` **4**, `alarms_raised` **2** (pump-01 WARNING y CRITICAL), `messages_dead_lettered` 0 |

**Conclusión:** no hay rechazo, así que la validación del producto no se debilita en ningún caso. Pero un activo sintético **no ejercita la máquina de estados**: el consumer haría menos trabajo por mensaje y el throughput medido saldría optimista.

**Propuesta para generar N activos (D-A):**

| Opción | Qué es | Coste |
|---|---|---|
| (a) Los 5 activos reales a más de 1 msg/s por activo | Sin cambios de config. El consumer hace el trabajo completo. | Tasa por activo poco realista (un sensor real muestrea a 1 Hz); solo 2 de las 3 particiones reciben datos (propuesta, 3.2) |
| (b) **N activos clonados mediante una config generada** | El arnés genera, a partir de `config/benchmark.yaml` (N), un directorio de config con N clones de los 5 activos reales (por ejemplo `pump-01-s007`), con sus `asset_overrides` copiados (`motor_current_a` no tiene umbral por defecto: sin override, la config no carga). Se monta de solo lectura sobre la config de los contenedores en las corridas de throughput. | Ningún cambio de código ni de validación: los dos loaders ya aceptan `config_dir` (`config/loader.py:510`, `config_loader.js:207`) y en contenedor basta con un bind mount. Requiere E1. La config generada es un derivado de `config/`, no una segunda fuente. |
| (c) N activos desconocidos | Funciona hoy | Throughput optimista: el consumer no evalúa reglas. No recomendado. |

**Recomendación: (b) para throughput; los 5 reales a 1 Hz para latencia.**

Observación de producto, sin cambio propuesto: que el bridge no compruebe que el activo existe es el comportamiento actual, coherente con su docstring (valida contra el esquema y el topic). Si debería rechazar activos desconocidos es una decisión de producto aparte; no la toca este step.

## 1.1.2 Observador WebSocket de ThingsBoard

Prueba de concepto: unos 8 minutos de trabajo de los 60 permitidos (inicio 21:54:08). Scripts: `scripts/bench/diagnostics/ws_observer_poc.js` (cliente `WebSocket` integrado en Node 24, sin dependencias) y `ws_poc.py`. Datos: `scripts/bench/diagnostics/results/ws-poc/`.

**Método:**
- Perfil full (`make up`).
- Un dispositivo de prueba `bench-ws-probe-01` de tipo `pump`, creado por el gateway del pipeline. Así toma el perfil `pump` y la cola PipelineSequential, igual que los dispositivos del pipeline.
- El observador se autentica con el JWT del tenant en `/api/ws` (`authCmd` y luego un comando `TIMESERIES` con `scope: LATEST_TELEMETRY`).
- El publicador envía 600 valores a 10 msg/s por la API MQTT del gateway (el camino del sink, QoS 1), en lazo abierto. Cada valor lleva `ts` igual al instante programado y `seq` igual a su índice.
- Al final se borra el dispositivo. Ninguna credencial se imprime ni se guarda: el JWT pasa al observador por variable de entorno.

**Resultado** (`results/ws-poc/summary.json`):

| Comprobación | Valor |
|---|---|
| PUBACK | 600 / 600 |
| Recibidos por el observador | **600**, 600 `seq` distintos, **0 perdidos, 0 duplicados** |
| `ts` exacto | 600 / 600 (0 erróneos) |
| Orden | en orden |
| Puntos por actualización | 1 (ThingsBoard no agrupa a esta tasa) |
| Retraso del publicador | p99 0,77 ms, máximo 5,4 ms |
| De `ts` a la llegada al observador | p50 19,4 ms, p95 31,4 ms, mínimo 3,8 ms, máximo 124,9 ms (orientativo, no publicable: reloj del host, publicación directa sin el sink) |

**Veredicto: el observador funciona.** F6 se mantiene. Dos precisiones para el arnés:
- La primera respuesta a la suscripción trae un marcador con valor vacío, fechado en el momento de suscribirse; el filtro `ts ≥ inicio de la corrida` lo descarta.
- En el benchmark, el observador debe correr en un contenedor de la VM, con el mismo reloj que el generador (propuesta, 2.3).

## 1.1.3 Diseño de la contenerización (E1)

### Imágenes

| Imagen | Base (versión fijada) | Contenido | Usuario |
|---|---|---|---|
| `industrial-telemetry/pipeline-python` | `python:3.13.4-slim-bookworm` (la misma versión que `.python-version`; además fijada por digest en el Dockerfile) | `requirements.txt` instalado; `common/`, `config/`, `schemas/`, `bridge/`, `edge/`, `broker/`, `processing/thingsboard_sink/`. Una sola imagen para el bridge, el sink, el simulador y el provisionado (comando distinto por servicio): un solo build y una sola capa de dependencias. | `app` (uid 10001, sin shell de login) |
| `industrial-telemetry/alarm-consumer` | `node:24.16.0-bookworm-slim` (la misma que `.nvmrc`; por digest) | `npm ci --omit=dev`; `common/jsonlog.js`, `config/`, `schemas/`, `processing/alarm_consumer/` | `node` (uid 1000, el de la imagen) |

- **`.dockerignore` estricto:** `.env`, `.venv/`, `node_modules/`, `tests/`, `docs/`, `scripts/bench/`, `bench/results/`, `.git/`. Ninguna credencial entra en una imagen.
- **Credenciales:** el token del gateway llega al sink por `environment: TB_GATEWAY_TOKEN: ${TB_GATEWAY_TOKEN}`, interpolado desde `.env`; el compose no contiene ningún valor.
- **Tamaños estimados**, no medidos: unos 230 MB la de Python (la base pesa 181 MB, y la imagen de diagnóstico con confluent-kafka, 215 MB) y unos 250 MB la de Node.
- **Señales:** `init: true` en el compose, para que SIGTERM llegue al proceso. El bridge, el consumer y el sink ya tratan SIGTERM (`test_signals` lo verifica en Linux para el bridge y el simulador).

### Perfil y `make`

- **Perfil nuevo `pipeline`**, con `bridge` y `alarm-consumer`; el sink va en otro perfil, `pipeline-tb` (ver abajo). Dentro de la red usan `mosquitto:1883`, `broker:29092`, `schema-registry:8081` y `thingsboard:1883`, por las variables de entorno que el código ya lee (`MQTT_HOST`, `KAFKA_BOOTSTRAP`, `SCHEMA_REGISTRY_URL`, `TB_HOST`, `TB_MQTT_PORT`).
- **El sink en perfil propio (`pipeline-tb`)**, porque depende de ThingsBoard y de la importación. Arrancarlo sin ThingsBoard lo dejaría reintentando sin fin.
- **Orden de arranque:** el bridge y el consumer fallan si los topics no existen. Dos opciones (D-C):
  - (i) el Makefile encadena: `up` o `up-core` (que provisionan desde el host) y después `docker compose --profile pipeline up -d`;
  - (ii) un servicio de un solo uso, `provision`, en el compose, con `depends_on: condition: service_completed_successfully`.
  
  Recomiendo (i): no cambia el flujo actual y es menos código. Lo mismo vale para la importación de ThingsBoard antes del sink.
- **Qué hace `make up` por defecto (D-B):**

  | Opción | Qué es | Coste |
  |---|---|---|
  | (a) Igual que hoy (infraestructura), más un objetivo nuevo, `make up-pipeline`, que levanta todo (full, provisión, importación y los perfiles `pipeline` y `pipeline-tb`) | Retrocompatible | Quien clone necesita conocer un objetivo más; queda en la ayuda |
  | (b) `make up` levanta también el pipeline en contenedores | Una sola orden para tener el sistema funcionando | Rompe el flujo de desarrollo: un bridge en el host más el del contenedor recibirían **los dos** cada mensaje MQTT (suscripción no compartida) y lo escribirían dos veces en Kafka |

  **Recomendación: (a).** Con (a), además, `make up-pipeline-core` (core y `pipeline`) sirve para el benchmark del pipeline.
- **Smoke en contenedores:** `make smoke-containers`, el equivalente al 200 de 200 actual. El simulador corre como `docker compose run --rm simulator --count 200 --seed 42 --fault pump-01:imbalance:20` con la imagen de Python, y la comprobación se hace contra Kafka (registros en `telemetry.raw`, alarmas, DLQ = 0) y los logs de los contenedores.

### Convivencia con los procesos en el host

- **El flujo actual no cambia:** `make up` / `up-core` sin el perfil `pipeline`, y los procesos lanzados a mano en el host.
- **Tests de integración:** el fixture levanta solo core o full y lanza sus propios procesos en el host. Como el perfil `pipeline` nunca se activa ahí, no hay conflicto. Ya se niega a empezar si hay un stack corriendo (`tests/integration/conftest.py:65`).
- **Riesgo:** perfil `pipeline` arriba y además procesos en el host. Mitigación propuesta: que los objetivos de `make` que lanzan procesos en el host (`smoke`) comprueben con `docker compose ps` que el perfil `pipeline` no está activo, y se nieguen si lo está. La alternativa, una suscripción compartida de MQTT (`$share/…`) en el bridge, cambiaría la semántica del producto y no la propongo aquí.

### CI

- **El job `integration` actual cambia (corrección del 2026-10-10):** además de probar los procesos en el host, ahora construye las dos imágenes, porque `tests/integration/test_containers.py` (perfil core) arranca el pipeline en contenedores. El build se añade a su duración. `checks` no cambia. `integration-full` también construye las dos imágenes: ejecuta los dos tests de contenedores, incluido el del sink, que es del perfil full.
- **Job nuevo propuesto, `pipeline-containers`** (`needs: checks`): build de las dos imágenes, `make up-pipeline-core`, `make smoke-containers` y `make down`.
- **Tiempo estimado, no medido:** build en frío de 1 a 2 minutos (descargas de unos 45 y 70 MB de bases, más pip y npm); con la caché de buildx de GitHub (`cache-from: type=gha`), de 15 a 30 s. Más el stack core (~30 s) y el smoke (~1 min), unos 3 a 4 minutos por push en total.
- **D-D:** job nuevo, o un paso más al final del job `integration`, que comparte el runner pero alarga ese job.

### Reparto de CPU (para el benchmark)

Hoy la VM tiene 4 CPU (`.wslconfig: processors=4`) de los 12 hilos del i7-10750H (6 núcleos), y ningún servicio tiene límites.

| Servicio | Necesidad estimada | Propuesta |
|---|---|---|
| broker | 1–2 CPU | 2 |
| mosquitto | 1 (un solo hilo) | 1 |
| bridge | 1 (procesado en un hilo de Python) | 1 |
| alarm-consumer | 1 (Node, un solo hilo) | 1 |
| generador | 1 por proceso (puede necesitar varios a tasas altas) | 1 |
| medidor (consumidor de `telemetry.raw`) | ≤ 0,5 | compartido con el generador |
| schema-registry, zookeeper | casi en reposo | 0,5 + 0,5 |
| margen (kernel, dockerd) | — | 1 |
| **Total** | | **8** |

- **`.wslconfig` recomendado: `processors=8`.** Le quedan 4 hilos a Windows. Lo aplica Anthony; yo no lo toco.
- **Mecanismo de límite (D-E):**
  - (i) `cpuset` por servicio (vCPU dedicadas): aísla sin la cuota CFS, cuyo estrangulamiento añadiría picos de latencia propios;
  - (ii) `cpus:` (cuota CFS): más simple, pero un servicio que se pasa de su cuota queda parado hasta el siguiente periodo de 100 ms, y eso contamina el p99.
  
  **Recomendación: (i) `cpuset`**, en un override solo para el benchmark (`docker-compose.bench.yml`), con el reparto declarado en los resultados. El uso normal no lleva límites.
- **Riesgo térmico:** 8 vCPU sostenidas en un portátil pueden hacer bajar la frecuencia. El arnés registra la frecuencia, si es accesible, y la invalidación por tiempo cubre los casos extremos. **Sin verificar.**

## 1.1.4 Diseño del generador y del arnés

### Parámetros: `config/benchmark.yaml` (fuente única)

Un fichero nuevo, validado por `config/loader.py` (con tests), con:
- la escalera de tasas;
- la duración del calentamiento (30 s) y del intervalo medido (300 s);
- las repeticiones (E2: 1 por escalón y 5 en el candidato y en el siguiente);
- los umbrales de D8 (p99 < 1 000 ms, criterio de lag y pérdidas = 0);
- los umbrales del generador (retraso p99 / máximo) y de CPU;
- el modo de activos (`real` o `cloned:N`);
- el patrón de fallos para F2;
- `bridge_linger_ms` (por defecto, ninguno: el de librdkafka; D2);
- el reparto de `cpuset`;
- la tasa del sink para F5.

Ningún script lleva constantes de benchmark.

### Módulos (paquete `bench/`)

| Módulo | Dónde corre | Qué hace |
|---|---|---|
| `bench/generator` | contenedor (imagen de Python) | Publica a Mosquitto en lazo abierto: el mensaje i está previsto en `t0 + i / tasa` sobre `time.monotonic()`. **`ts` = instante programado** (pared de la VM, en ms). Registra aparte su retraso (`real − previsto`) en un histograma. QoS 1 con `max_inflight` de paho ampliado, para que la ventana de acks no frene el envío. Se puede repartir en varios procesos por activo si una sola CPU no llega. Aplica el patrón de fallos de F2 (onda cuadrada de temperatura por activo, que dispara RAISED y CLEARED de forma determinista). |
| `bench/measurer` | contenedor | Consumidor de `telemetry.raw`: decodifica el Avro, registra `visible − ts` por registro en un histograma y lleva el conjunto de `(asset, seq)` para la integridad (huecos y duplicados). Es el punto extremo a extremo de D8, sin depender de los logs del bridge. |
| `bench/ws_observer` | contenedor (Node) | El observador de 1.1.2, para F6 (perfil full). |
| `bench/harness` | host Windows (necesita la energía y el registro de eventos) | Orquesta cada corrida (ver abajo) y escribe los resultados. |
| `bench/report` | host | Genera las tablas de F1 a F7 desde `bench/results/`, con los umbrales de `config/benchmark.yaml`, y puede recalcular D8 con otros umbrales sin repetir corridas. |
| `bench/bridge_linger` | contenedor | Solo si D-F (i): ver abajo. |

### Una corrida

1. Comprobaciones previas: corriente alterna, `docker ps` sin contenedores ajenos y entorno registrado (CPU, RAM, `.wslconfig`, versiones de Docker y del kernel, plan de energía, commit y si el árbol está sucio).
2. Recrear el stack (`make down` y `make up-pipeline-core`, o full para F5–F7) con el override de CPU y, si toca, la config de activos clonados montada.
3. Watermarks iniciales de `telemetry.raw`, `telemetry.alarms` y `telemetry.dlq`.
4. Arrancar el medidor y luego el generador. Calentamiento de 30 s, intervalo medido de 300 s, fin del envío y drenaje hasta lag 0 (con tiempo máximo).
5. Durante toda la corrida, un monitor en el host muestrea cada 5 s la corriente, `docker ps`, `cpu.stat` de cada contenedor (incluido `nr_throttled`) y el lag de los grupos.
6. Recogida: histogramas, series por segundo, logs de los contenedores (solo eventos de alarma y resúmenes), contadores y watermarks finales.
7. Validez e integridad (abajo) y escritura de `manifest.json`.

### Formato de resultados: `bench/results/<fecha>-<id>/`

| Fichero | Contenido |
|---|---|
| `manifest.json` | Entorno, parámetros efectivos (copia de `benchmark.yaml` y de la config de activos usada), commit, inicio y fin, `valid` (true/false) con la lista de motivos e indicador `publishable` (false en las corridas de prueba) |
| `latency_<métrica>.json` | Histograma de resolución fija, log-lineal: cubos de 0,1 ms hasta 10 ms, de 1 ms hasta 1 s y de 10 ms hasta 60 s. Cualquier percentil y cualquier umbral (D8) se recalculan con un error menor de un cubo. Uno por métrica: `e2e_visible`, `alarm_e2e`, `generator_late` y, si aplica, `produce_to_ack` y `tb_visible`. |
| `series.csv` | Por segundo: enviados, visibles, lag de cada grupo, p99 del segundo, CPU por contenedor y alimentación |
| `counters.json` | Enviados por activo, registros nuevos por topic, DLQ, duplicados, huecos, alarmas esperadas frente a emitidas |
| `raw/` (no versionado) | Muestras crudas comprimidas, solo en local (`.gitignore`): a 10 000 msg/s son decenas de MB por corrida |

### Cómo se calcula cada cifra

| Cifra | Cálculo | Fuente |
|---|---|---|
| F1, latencia del bridge | `visible − ts`, con `ts` = instante programado, a la tasa fija declarada; p50/p95/p99 del histograma `e2e_visible` | medidor. **Desvío propuesto respecto a la propuesta:** "visible para un consumidor de `telemetry.raw`" en lugar de "ack del bridge". Leer el ack de los logs exige un log INFO por mensaje, que a tasa alta pesa en la CPU del bridge (es lo que se mide). La diferencia es el `fetch` del consumidor (`fetch.wait.max.ms`, que se declara) (D-G). |
| F2, extremo a extremo de alarma | `alarm_ack_ts_ms − ts` de la muestra que dispara (`event_to_alarm_ack_ms` del consumer) | logs de eventos de alarma del consumer (escasos). Generador y consumer comparten el reloj de la VM. |
| F3, throughput sostenible (D8) | La tasa más alta de la escalera con: p99 de `e2e_visible` < 1 s; lag acotado (pendiente de una regresión del lag del alarm consumer en el intervalo medido ≤ 0 y lag máximo menor que el umbral de la config); cero pérdidas (integridad). Con E2, 1 corrida por escalón y 5 en el candidato y en el siguiente. | `series.csv`, histogramas y contadores; recalculable |
| F4, coste de la escritura a Kafka | `produce_to_ack_ms` del bridge, solo en corridas de latencia a tasa baja con log INFO | logs del bridge |
| F5, techo del camino a ThingsBoard | Tasa por activo sostenida con el límite de 10 msg/s, sin crecer el lag del sink y sin pérdidas en ThingsBoard | lag del sink; recuento de puntos por dispositivo por REST; estadísticas de la cola |
| F6, latencia hasta ThingsBoard | `llegada al observador − ts`, filtrando el marcador inicial | observador WebSocket (1.1.2) |
| F7, tiempo de drenaje | Backlog de N mensajes por activo, de ThingsBoard parado a lag 0 | lag del sink |

### Validez (cualquier fallo invalida la corrida)

- **Corriente alterna** en todas las muestras del monitor.
- **Suspensión:** ningún evento Kernel-Power 42/107 en la ventana de la corrida (registro de sistema de Windows); además, el reloj de pared avanza lo mismo que el monotónico (±1 s), y el retraso del generador no pasa su umbral.
- **Contenedores ajenos:** ninguno en ningún muestreo de `docker ps`.
- **CPU:** ningún cliente por encima del umbral; `nr_throttled` no crece durante el intervalo medido (con `cpuset` no debería haber estrangulamiento).
- **Stack recreado:** la hora de inicio de los contenedores es posterior al inicio de la corrida.

### Integridad

- Enviados por el generador = registros nuevos en `telemetry.raw` = `(asset, seq)` distintos vistos por el medidor.
- Duplicados contados aparte: at-least-once los permite, y se declaran.
- DLQ: registros nuevos = 0.
- Lag del alarm consumer = 0 tras el drenaje.
- Alarmas emitidas = las que predice el patrón de fallos.

### `linger.ms` del bridge sin tocar su código (D2) — D-F

El bridge no admite hoy ningún ajuste del productor: su configuración está fija en el código (`bridge/mqtt_to_kafka/__main__.py:192-201`). Cumplir D2 al pie de la letra requiere una de estas opciones:

| Opción | Qué es | Coste |
|---|---|---|
| (i) Lanzador del benchmark | `python -m bench.bridge_linger --linger-ms X` envuelve la clase `Producer` de confluent-kafka para añadir `linger.ms`, y luego llama a `bridge.mqtt_to_kafka.main()`. No toca el código del bridge. | Mecanismo de monkeypatch: frágil ante cambios de librería, aunque acotado al benchmark y probado |
| (ii) Ajuste genérico del productor en el bridge | Por ejemplo, `KAFKA_PRODUCER_LINGER_MS`, leído en `start_kafka()` | Un cambio de producto pequeño, con tests; **contradice** "sin tocar el código del bridge" |

**Recomendación: (i)**, que respeta el briefing; (ii) si prefieres tenerlo como función del producto.

## Decisiones que necesita Anthony

| # | Decisión | Opciones | Recomendación |
|---|---|---|---|
| D-A | Cómo generar N activos para throughput | (a) 5 reales a más de 1 Hz; (b) N clones mediante una config generada y montada; (c) activos desconocidos | (b) |
| D-B | Qué hace `make up` | (a) igual que hoy, más `make up-pipeline`; (b) levanta también el pipeline | (a) |
| D-C | Orden de arranque (topics e importación antes del pipeline) | (i) encadenado en el Makefile; (ii) servicios de un solo uso en el compose | (i) |
| D-D | CI de los contenedores | job nuevo `pipeline-containers`; o un paso más en `integration` | job nuevo |
| D-E | Límite de CPU en el benchmark | (i) `cpuset`; (ii) `cpus:` (CFS); con `processors=8` en `.wslconfig` (lo aplica Anthony) | (i) y `processors=8` |
| D-F | Cambiar `linger.ms` del bridge por parámetro | (i) lanzador en `bench/`; (ii) ajuste en el bridge | (i) |
| D-G | Punto de medida de F1 y de D8 | "visible en `telemetry.raw`" (medidor) o "ack del bridge" (logs INFO por mensaje) | medidor |
| D-H | Muestras crudas | histogramas versionados y crudo solo en local; o crudo versionado | histogramas |

## Desvíos y pendientes

- El briefing da `78673d2` como estado de partida; el real era `9ace303`, con el briefing ya en la rama.
- El briefing supone que los activos sintéticos podrían rechazarse; el código los acepta (1.1.1).
- D2 no se puede cumplir sin código: el bridge no tiene parámetro de productor (D-F).
- F1 cambia de punto de medida si se aprueba D-G.
- Sin verificar: los tamaños de imagen y los tiempos de CI (estimados), el riesgo térmico con 8 vCPU, la tasa máxima de un generador Python de un solo proceso y el coste del log INFO por mensaje del bridge a tasa alta.

## Decisiones del DETENTE 3.3.b

Anthony aprobó el DETENTE 3.3.b con estos cambios. Prevalecen sobre las secciones anteriores donde difieran.

| # | Decisión |
|---|---|
| D-A | Aprobada: clones de los activos reales a partir de una config generada. |
| D-B | Aprobada: `make up` no cambia; se añade `make up-pipeline`. |
| D-C | Aprobada: el orden de arranque se encadena en el Makefile. |
| D-D | Aprobada: job nuevo de CI, `pipeline-containers`. |
| D-E | Aprobada y aplicada por Anthony: `.wslconfig` con `processors=8`, WSL reiniciado. Comprobado: `docker info` da 8 CPU y 12 543 291 392 B. `cpuset` por servicio solo en un override del benchmark. |
| D-F | **Cambiada: sin lanzador.** El bridge lee sus ajustes de productor de `config/` (fuente única), con valores por defecto idénticos al comportamiento actual (`acks=all`, idempotencia, y el resto los de librdkafka), y con tests unitarios. Esto levanta, solo para este cambio, la restricción del briefing de no tocar el bridge. Es lo que necesita el arnés (1.3) para lanzar una corrida con otro `linger.ms`. |
| D-G | **Aprobada con condición:** el medidor registra también el timestamp de productor (`CreateTime` de Kafka) de cada registro, para separar la parte del bridge y la parte de lectura de F1. |
| D-H | Aprobada: se versionan los histogramas de resolución fija; las muestras crudas, solo en local. |

### Nueva definición de F1 (D-G)

**F1 = `t_visible − ts`**, donde:
- `ts` es el instante programado de la muestra por el generador;
- `t_visible` es el instante (reloj de pared de la VM) en que el medidor, un consumidor de `telemetry.raw` en un contenedor de la misma VM, recibe el registro.

Se publica como p50/p95/p99 a una tasa fija declarada, junto con `fetch.wait.max.ms` del medidor. Se descompone en dos partes, que suman F1:
Nombres corregidos en el DETENTE 3.3.d: la "parte del bridge" pasa a llamarse **`ingest`** y la "parte de lectura", **`produce_to_visible`**. Ver la sección del 3.3.d.

- **Parte del bridge (`ingest`): `CreateTime − ts`.** `CreateTime` es el timestamp que el productor del bridge pone en el registro al llamar a `produce()`, en milisegundos y con el reloj del contenedor del bridge, que es el mismo que el de la VM (propuesta, 2.3). Incluye MQTT, la cola del bridge, la validación y la serialización. Lo que tarda el productor en enviar y obtener el ack (linger incluido) **no** está en esta parte.
- **Parte de lectura (`produce_to_visible`): `t_visible − CreateTime`.** Incluye el linger, el envío, el append en el broker y el fetch del medidor.

Los dos timestamps tienen resolución de 1 ms (`ts` lo trunca el generador, y `CreateTime` es un entero de ms), así que cada parte tiene ±1 ms de error de cuantización.

### Resolución de los histogramas (D-H)

Cubos fijos, log-lineales, iguales en todas las corridas y métricas:
- **0,1 ms** de 0 a 10 ms (100 cubos);
- **1 ms** de 10 ms a 1 s (990 cubos);
- **10 ms** de 1 s a 60 s (5 900 cubos);
- un cubo de desbordamiento por encima de 60 s.

Un percentil, o la fracción por debajo de un umbral (D8), se recalcula con un error menor que la anchura del cubo en que cae: 0,1 ms, 1 ms o 10 ms. El umbral de D8 (1 s) cae justo en un borde de cubo.

### Requisitos añadidos al arnés (1.3)

1. **Generador saturado.** Si el retraso propio del generador supera un límite fijado en `config/benchmark.yaml`, la corrida es inválida. Si el generador es el cuello de botella de la escalera de throughput, el resultado se informa como "limitado por el generador", no como techo del pipeline.
2. **Riesgo térmico.** En cada corrida se registra el uso de CPU por contenedor, y el plan de corridas incluye pausas de enfriamiento entre repeticiones.

### Alcance de la 1.2

La 1.2 implementa la contenerización (D-B a D-D). D-F, D-G, los requisitos añadidos y D-A (la config generada) se implementan en la 1.3, junto con el generador y el arnés.

## Decisiones del DETENTE 3.3.d

Anthony aprobó el DETENTE 3.3.d con estas correcciones. Prevalecen sobre el resto del documento.

### 1. Nivel de log del bridge

- **Toda corrida usa INFO, el valor por defecto del producto** (`bridge/mqtt_to_kafka/__main__.py:138`), que registra una línea `message_delivered` por mensaje (`:357`). Esto incluye la escalera de throughput, cuya cifra es la principal: mide el producto tal como se distribuye. `config/benchmark.yaml`: `bridge.log_level_latency` y `log_level_throughput` valen INFO.
- **El plan original medía la escalera en WARNING** (`log_level_throughput: WARNING`), para que el log no cargara al bridge. Se cambia porque así se medía un bridge configurado de forma distinta al producto, y la cifra publicada no habría correspondido a lo que ejecuta quien clona el repo.
- **Bloque de sensibilidad nuevo:** tras E2, el escalón candidato y el siguiente se repiten con el bridge en WARNING (`bridge.log_level_sensitivity`), `throughput.sensitivity_repetitions` veces cada uno (1 en la config). Sirve para cuantificar el coste del log por mensaje. El arnés lo lanza con `run --mode throughput --sensitivity`, y el informe lo compara con la escalera.
- **El alarm consumer** corre en INFO tanto en el producto como en el benchmark, y no registra nada por mensaje: solo una línea por alarma.

### 2. Descomposición de F1: nombres

- **`ingest` = `CreateTime − ts`:** publicación MQTT, Mosquitto, cola del bridge, validación y serialización, hasta la llamada a `produce()`.
- **`produce_to_visible` = `t_visible − CreateTime`:** el `linger.ms` de librdkafka, la petición de produce, el append en el broker con el avance del high watermark, y el fetch del medidor. No espera a que el ack vuelva al bridge, así que no es lo mismo que F4 (`produce_to_ack`). **Lo domina `linger.ms`** (5 ms por defecto).
- `CreateTime` lo fija librdkafka al llamar a `produce()`: el bridge no pasa ningún `timestamp`, y el valor por defecto de confluent-kafka es "current time".
- Los ficheros pasan a ser `latency_ingest.json` y `latency_produce_to_visible.json`. El informe explica qué contiene cada parte.

### 3. Calibración del generador y del medidor

El primer bloque del STEP 2 mide la tasa máxima que sostienen el generador y el medidor por separado (`config/benchmark.yaml: calibration`):
- **Generador** (`run --mode calibrate-generator --rate R`): publica a Mosquitto sin ningún bridge suscrito, a cada tasa de `calibration.generator_rates_per_s`. Sostiene una tasa si su retraso queda dentro de `generator.max_late_*` y Mosquitto confirma todos los mensajes.
- **Medidor** (`run --mode calibrate-meter`): `bench/loader.py` precarga `calibration.meter_records` registros en `telemetry.raw`, y el medidor los lee desde el principio (`--from-beginning --expect N`). Su tasa es registros entre segundos, del primero al último.
- **La escalera se recorta** a la menor de las dos tasas máximas (`criteria.cap_ladder`), y el informe lo indica: una cifra en el tope está limitada por las herramientas de medida, no por el pipeline.

### 4. Resolución de la escalera

Cuando la escalera encuentra el último escalón que pasa y el primero que falla, se corre **un escalón intermedio** entre ambos antes de las repeticiones E2 (`criteria.intermediate_step`: su punto medio, redondeado a múltiplo de 50 msg/s cuando cae estrictamente entre los dos). El informe indica cuál falta.

### 5. Camino a ThingsBoard

- **El STEP 2 corre solo F1 a F4.**
- **F5 y F6** tendrán después un modo reducido del arnés, en un step aparte.
- **F7** (tiempo de drenaje tras una caída de ThingsBoard) pasa al backlog (`docs/internal/phase3_backlog.md`): necesita una parada controlada de ThingsBoard, que ignora SIGTERM.

### 6. Notas para el informe de cierre de la Task 3.3

Registradas en `docs/internal/phase3_backlog.md`, sección del informe de cierre:
- las funciones de E/S del arnés llevan `pragma: no cover`, así que la cifra de cobertura no las incluye;
- la tolerancia de la pendiente del lag es de 0,001 s/s.

