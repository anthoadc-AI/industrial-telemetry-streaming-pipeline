# Task 3.3 STEP 0 — Propuesta de protocolo de medición

Estado: **aprobado en el DETENTE 3.3.a (2026-10-06), con D1–D9 según la recomendación** de la tabla final, **modificado por las enmiendas del 2026-10-09** (sección al final), que prevalecen sobre el resto del documento. Ninguna cifra de este documento es publicable: son diagnósticos de una sola máquina, tomados para diseñar el protocolo.

## 0. Verificación de estado

### 0.1 Repositorio y CI

- Rama `feature/phase3-benchmark` en `faa237d`, un commit por delante de `main` (`a895bb3`), árbol limpio, sin upstream (esperado; no se sube). Evidencia: `git status --short --branch`, `git rev-list --count main..HEAD` = 1.
- Último run de CI en `main`: 37234433277 sobre `a895bb3`, **success** (`gh run list --branch main`).

### 0.2 Contenedores

- Ninguno corriendo al empezar (`docker ps` vacío). Antes de cada celda de la sección 1, `run_matrix.py` vuelve a mirar `docker ps` y se detiene (exit 3) si ve algo que no sea del proyecto `industrial-telemetry` o de los propios diagnósticos (`bench-diag-*`). No se detuvo nunca.
- Hay 19 contenedores parados, de otros cursos. No se tocaron. Política de reinicio (`docker inspect`, `HostConfig.RestartPolicy`):

| Contenedor (nombre omitido) | Imagen | Proyecto compose | Restart | Parado desde |
|---|---|---|---|---|
| otro-01 | eclipse-mosquitto | otro proyecto | no | 2026-08-24 |
| otro-02 | bitnamilegacy/spark:3.5 | otro proyecto | no | 2026-07-23 |
| otro-03 | bitnamilegacy/spark:3.5 | otro proyecto | no | 2026-07-23 |
| otro-04 | mysql:8.0 | — | no | 2026-07-04 |
| otro-05 | apache/nifi:1.13.2 | — | no | 2026-07-04 |
| otro-06 | cassandra | — | no | 2026-07-02 |
| otro-07 | mongo | — | no | 2026-07-02 |
| otro-08 | mysql:8.0 | — | no | 2026-07-02 |
| otro-09 | apache/nifi:1.13.2 | — | no | 2026-07-02 |
| otro-10 | imagen local | — | no | 2026-06-22 |
| otro-11 | imagen local | — | no | 2026-06-24 |
| otro-12 | imagen local | — | no | 2026-06-11 |
| otro-13 | imagen local | — | no | 2026-06-11 |
| otro-14 | ubuntu | — | no | 2026-06-08 |
| otro-15 | redis | — | no | 2026-07-02 |
| otro-16 | mysql:8.0 | — | no | 2026-05-22 |
| otro-17 | cassandra:latest | — | no | 2026-05-19 |
| otro-18 | node:20-alpine | otro proyecto | **unless-stopped** | 2026-05-18 |
| otro-19 | hello-world | — | no | 2026-05-07 |

  Solo `otro-18` tiene política de reinicio (`unless-stopped`). Como se paró a mano, Docker no lo arranca solo, pero un `docker compose up` de su proyecto sí lo haría. Los contenedores de Airflow y Hadoop que menciona el briefing **no existen** en esta máquina porque Anthony los bajó el 2026-10-05 con `docker compose -p <proyecto> down`, sin `-v`, conservando sus volúmenes. Quedan sus imágenes (`apache/airflow:2.1.1`, `apache/airflow:2.3.0` y `bde2020/hadoop-*`) y sus volúmenes, de los proyectos `airflow-docker`, `airflow_assignment`, `airflow` y `docker-hadoop` (`docker volume ls`, comprobado el 2026-10-09).

### 0.3 Entorno

| Elemento | Valor | Evidencia |
|---|---|---|
| CPU | Intel Core i7-10750H @ 2.60 GHz, 6 núcleos / 12 hilos | `Win32_Processor` |
| RAM | 15,8 GB | `Win32_ComputerSystem` |
| `.wslconfig` | `memory=12GB`, `processors=4`, `localhostForwarding=true` | `~/.wslconfig` |
| VM de Docker | 4 CPU, 11,68 GiB (12 544 360 448 B) | `docker info` |
| Docker Desktop | 4.53.0 (211793); Engine 29.0.1; Compose v2.40.3-desktop.1 | `docker version`, `docker compose version` |
| Backend | WSL 2 (distro `docker-desktop`, versión 2); WSL 2.6.3.0 | `wsl -l -v`, `wsl --version` |
| Kernel de la VM | 6.6.87.2-microsoft-standard-WSL2; cgroup v2, driver cgroupfs; overlayfs | `docker info` |
| Distro WSL de usuario | ninguna (solo `docker-desktop`) | `wsl -l -v` |
| Windows | 11 Home 10.0.26200 | `wsl --version` |
| Plan de energía | plan del fabricante (`6d85d030-…`); estado mínimo del procesador 0 %, máximo 100 % (AC y batería) | `powercfg /getactivescheme`, `/query … PROCTHROTTLEMIN/MAX` |
| **Alimentación** | Batería en las pasadas 1 (sus 4 primeras celdas) y 3; **corriente alterna en la pasada 4 (la de referencia), en `bimodal-ac` y en las mediciones de reloj** | `SystemInformation.PowerStatus`, `GetSystemPowerStatus` y `Win32_Battery.BatteryStatus`, registrado antes y después de cada celda en `cells.jsonl` |
| Resolución del temporizador de Windows | actual 1,0 ms (la más gruesa 15,625 ms, la más fina 0,5 ms) | `NtQueryTimerResolution`, en el resumen de cada celda del host |

Las cifras de referencia de la sección 1 (pasada 4 y `bimodal-ac`) se midieron en corriente alterna, con cada celda validada antes y después. Las pasadas en batería se conservan etiquetadas (1.5, 6.5). El protocolo exige corriente alterna (5.1.2).

Imágenes del compose (`docker-compose.yml`): `eclipse-mosquitto:2.1.2-alpine`, `confluentinc/cp-zookeeper:6.1.1`, `confluentinc/cp-server:6.1.1`, `confluentinc/cp-schema-registry:6.1.1` y `thingsboard/tb-postgres:4.2.1.1` (perfil full).

Clientes Kafka:

| Componente | Librería | Configuración de productor (efectiva) |
|---|---|---|
| Bridge (Python) | confluent-kafka 2.15.1, librdkafka 2.15.1; Python 3.13.4 | `acks=all`, `enable.idempotence=true` (`bridge/mqtt_to_kafka/__main__.py:192-201`). Todo lo demás, por defecto. |
| Sink ThingsBoard, productor de DLQ (Python) | igual | `acks=all`, `enable.idempotence=true` (`processing/thingsboard_sink/__main__.py:295-296`) |
| Alarm consumer (Node.js 24.16.0) | kafkajs 2.2.4 | `idempotent: true`, `maxInFlightRequests: 1`, `acks: -1` en cada `send`, retries 5, `AssetPartitioner` (`processing/alarm_consumer/consumer.js:117-125, 370-381`) |

Valores por defecto que aplican, y cómo se comprobó cada uno:

- **librdkafka.** Con `debug=conf`, librdkafka imprime solo lo que difiere del defecto: `enable.idempotence = true` y `request.required.acks = -1`. Ni `linger.ms`, ni `batch.*`, ni `socket.nagle.disable`, ni `compression.*` aparecen, así que están en su valor por defecto. Esos valores, según la documentación de librdkafka 2.15.1 (CONFIGURATION.md), son `linger.ms` = 5, `batch.num.messages` = 10000, `batch.size` = 1000000, `socket.nagle.disable` = false (Nagle activo), `compression.codec` = none y `max.in.flight` 1000000 (con idempotencia, 5). La librería no expone una forma de leer el valor efectivo, así que estos números son los documentados, **no leídos**. El experimento `linger0` de la sección 1 confirma indirectamente que el linger por defecto no es 0.
- **kafkajs.** No tiene linger: cada `send` es una petición. Compresión: ninguna (no se pasa `compression`). Nagle: `node_modules/kafkajs/src/network/socketFactory.js` solo llama a `setKeepAlive` y nunca a `setNoDelay`, así que rige el defecto de `net.Socket` de Node (`noDelay: false`, Nagle activo según la documentación de Node). **No verificado empíricamente.**

### 0.4 Inventario de lo que ya se mide

| Métrica | Proceso | Desde → hasta | Reloj | Dónde |
|---|---|---|---|---|
| `event_to_receive_ms` | bridge | `ts` del mensaje (simulador, `datetime.now`, truncado a ms) → `on_message` del bridge (`time.time_ns`) | **pared**, entre procesos (simulador y bridge, mismo host) | `bridge/mqtt_to_kafka/__main__.py:323, 341` |
| `receive_to_produce_ms` | bridge | recepción (`perf_counter` en `on_message`) → justo antes de `produce()` | monotónico | `:323, 294, 351` |
| `produce_to_ack_ms` | bridge | antes de `produce()` → callback de entrega, en el hilo de delivery reports | monotónico (`perf_counter`) | `:294, 350, 352` |
| `event_to_ack_ms` | bridge | suma de las tres anteriores | mezcla: pared + monotónico | `:359` |
| `receive_ts_ms` | bridge | instante absoluto de recepción, para cruzar con un benchmark | pared (`time_ns`) | `:342` |
| `consume_to_alarm_ack_ms` | alarm consumer | el handler recibe el mensaje → se resuelve `await send` de la alarma | monotónico (`process.hrtime.bigint`) | `processing/alarm_consumer/consumer.js:264, 309, 316` |
| `event_to_alarm_ack_ms` | alarm consumer | `ts` de la muestra (simulador) → ack de la alarma | **pared**, entre procesos; el lado Node usa `performance.timeOrigin + performance.now()`, fijado al arrancar, que no sigue ajustes posteriores del reloj del sistema | `consumer.js:58-76, 317` |
| `alarm_ack_ts_ms` | alarm consumer | instante absoluto del ack | pared de alta resolución (la misma) | `consumer.js:318` |
| `hr_minus_date_now_ms` | alarm consumer | divergencia entre ese reloj y `Date.now()`, al arrancar y al terminar | — | `consumer.js:73-77` |
| `rate_limited_ms` | sink ThingsBoard | tiempo total esperando el limitador de tasa | monotónico | `processing/thingsboard_sink/__main__.py:116, 363` |
| QoS 1 sent / acked | simulador | contadores, sin tiempos | — | `edge/simulator/__main__.py:121-145` |

Ni el sink ni ThingsBoard miden latencia por mensaje. El reloj de pared del host en Python es `GetSystemTimePreciseAsFileTime`, con resolución de 1 µs (`time.get_clock_info("time")`, registrado por `clock_offset.py`).

**Desvío:** el briefing nombra `ts_to_ack_ms`, que ya no existe. Se eliminó en `fe37d8f` (Task 3.1-A) y no se redefinió (docstring del bridge, líneas 70-72).

## 1. Pregunta 1 — la discrepancia de produce_to_ack_ms

### 1.1 Diseño de la matriz

Scripts (desechables) en `scripts/bench/diagnostics/`; datos crudos en `results/`:

- `produce_ack_probe.py` reproduce el productor del bridge: misma configuración (`acks=all`, idempotencia), mismo hilo de delivery reports en `poll(0.2)` y `perf_counter` antes de `produce()` y en el callback. No usa MQTT ni Avro; el payload son 72 bytes aleatorios, el tamaño de un registro de telemetría Avro (65–77 B medidos con `fastavro`). Produce a un topic scratch, `bench.diag.produce-ack` (3 particiones). Registra también las estadísticas de librdkafka cada segundo: `int_latency` (de `produce()` a petición construida, incluye el linger), `outbuf_latency` y `rtt` (de petición a respuesta).
- `isolated_publisher.py` publica telemetría válida de un solo activo a 1 msg/s, para medir el bridge real con mensajes aislados (el simulador siempre publica 5 seguidos por ciclo).
- `run_matrix.py` ejecuta las celdas. Antes de cada una comprueba `docker ps` y registra la alimentación. Invalida la celda, y para, si el generador se retrasa más de 50 ms, si no está en corriente alterna o si hay errores.
- `bimodal.py` repite ráfagas cortas, alternando variantes, para medir con qué frecuencia aparece el modo lento (ver 1.3).
- Celdas: N = 220 mensajes, se descartan los 20 primeros, quedan n = 200.
- Datos por celda: `<celda>.csv` (una fila por mensaje), `<celda>.stats.csv` (estadísticas de librdkafka por ventana, reducidas por `reduce_stats.py`) y `summary.jsonl` / `cells.jsonl`. Los volcados completos de estadísticas (`*.stats.jsonl`, unos 20 MB) quedan solo en local (`results/.gitignore`).

| Celdas | Dónde corre el cliente | Cadencia |
|---|---|---|
| `anchor-*` | **el bridge real** en el host, con MQTT y Avro | ráfaga: simulador (5 mensajes seguidos por segundo); aislado: `isolated_publisher.py` |
| `host-*` | probe en el host Windows, `localhost:9092` (reenvío de puertos de Docker Desktop) | `iso` 1 msg/s; `burst` 5 seguidos por segundo; `singles10` 1 msg cada 100 ms |
| `ctr-*` | probe en un contenedor en la red del compose, `broker:29092` (sin reenvío de puertos) | igual |

Variables que se cambiaron de una en una respecto a la base: `nagleoff` (`socket.nagle.disable=true`), `linger0` (`linger.ms=0`) y `timer1ms` (`timeBeginPeriod(1)` en el proceso, solo en Windows). No hay distro WSL de usuario (0.3), así que esa ubicación no aplica.

### 1.2 Resultados: pasada 4 (`results/run4-ac`), en corriente alterna, 15 de 15 celdas válidas

`produce_to_ack_ms` en ms, recalculado de los CSV crudos por `analyze.py`. Las columnas de librdkafka son la mediana, por ventana de 1 s, de su p50.

| Celda | n | p50 | p95 | min | max | int_latency | outbuf | rtt |
|---|---|---|---|---|---|---|---|---|
| anchor-bridge-burst | 200 | 16.8 | 21.3 | 13.8 | 27.0 | — | — | — |
| anchor-bridge-iso | 200 | 16.1 | 23.3 | 7.2 | 27.4 | — | — | — |
| host-burst-base | 200 | **58.1** | 67.2 | 48.4 | 71.0 | 11.78 | 0.37 | **44.8** |
| host-iso-base | 200 | 15.2 | 21.6 | 6.7 | 23.9 | 12.99 | 0.22 | 1.52 |
| host-singles10-base | 200 | 14.6 | 22.2 | 6.5 | 26.4 | — | — | — |
| host-iso-nagleoff | 200 | 14.9 | 22.4 | 7.0 | 34.0 | 12.48 | 0.22 | 1.85 |
| host-iso-linger0 | 200 | **2.0** | 6.0 | 1.3 | 25.9 | 0.04 | 0.16 | 1.77 |
| host-iso-timer1ms | 200 | 14.8 | 23.1 | 6.6 | 25.9 | 12.57 | 0.25 | 1.52 |
| host-burst-nagleoff | 200 | 16.7 | 23.5 | 7.4 | 24.5 | 10.94 | 0.39 | 1.72 |
| host-burst-linger0 | 200 | **45.1** | 48.9 | 1.8 | 50.8 | 0.07 | 0.28 | **44.8** |
| ctr-burst-base | 200 | 6.4 | 8.4 | 5.8 | 10.3 | 5.18 | 0.03 | 0.71 |
| ctr-iso-base | 200 | 6.4 | 8.6 | 5.7 | 32.5 | 5.31 | 0.09 | 0.83 |
| ctr-singles10-base | 200 | 6.3 | 8.5 | 5.7 | 8.9 | — | — | — |
| ctr-iso-nagleoff | 200 | 6.2 | 8.2 | 5.7 | 9.1 | 5.25 | 0.09 | 0.73 |
| ctr-iso-linger0 | 200 | **1.0** | 2.9 | 0.7 | 3.1 | 0.11 | 0.09 | 0.69 |

(— : el bridge no registra estadísticas de librdkafka; las celdas `singles10` sí las registran, pero la ventana de descarte de 25 s cubre casi toda la celda, que dura 22 s.)

En cada celda, `produce_to_ack_ms` ≈ `int_latency` + `outbuf_latency` + `rtt`. Por ejemplo, `host-iso-base`: 13,0 + 0,2 + 1,5 = 14,7 frente a 15,2; `host-burst-base`: 11,8 + 0,4 + 44,8 = 57,0 frente a 58,1. El callback añade menos de 1 ms.

### 1.3 La ráfaga en el host es bimodal (`results/bimodal-ac`)

La celda `host-burst-base` dio p50 de 55 a 59 ms en unas pasadas y de 14 a 18 ms en otras (pasadas 1 a 4). Para no sacar conclusiones de una celda por variante, `bimodal.py` hizo 8 rondas de 5 variantes en ráfaga (110 mensajes por celda, 10 descartados), alternando el orden. Se cuenta como lenta una celda con p50 > 30 ms:

| Variante | Celdas | Lentas | p50 de cada celda (ms) |
|---|---|---|---|
| host-burst-base | 8 | 3 | 59.0, 16.0, 16.1, 16.6, 16.4, 56.4, 59.1, 12.9 |
| host-burst-nagleoff | 8 | 4 | 17.3, 12.0, 58.8, 57.8, 16.0, 14.7, 58.1, 60.0 |
| host-burst-linger0 | 8 | 4 | 43.5, 44.7, 4.4, 2.4, 2.2, 2.4, 42.8, 45.0 |
| host-burst-l0-nagleoff | 8 | 6 | 46.2, 2.8, 45.5, 45.7, 43.1, 2.2, 43.8, 45.3 |
| ctr-burst-base | 8 | **0** | 6.4, 6.7, 6.6, 7.1, 6.3, 6.4, 6.8, 6.2 |

- **El modo lento es una ida y vuelta de ~45 ms** (`rtt` 44,8 ms frente a 1,5–1,8 ms), no espera en el cliente: `int_latency` no cambia.
- **Se decide por conexión.** Cada celda abre una conexión nueva. Dentro de una celda todos los mensajes van en el mismo modo: p95 cerca del p50, y las cinco posiciones de la ráfaga con la misma mediana (58,9, 59,0, 59,0, 59,0 y 58,6 ms en `host-burst-base`).
- **Solo con ráfagas y solo en el host.** Aparece en 17 de las 32 celdas en ráfaga del host de `bimodal-ac`. No aparece en ninguna celda aislada del host con el linger por defecto: 12 celdas válidas en las 4 pasadas (bridge, `iso`, `singles10`, `nagleoff` y `timer1ms`), todas entre 14,6 y 16,9 ms de p50. Tampoco en ninguna celda del contenedor: 5 en la pasada 4 y 8 en `bimodal-ac`.
- Unos 40 ms extra son la firma del ACK retardado de TCP. Dónde se produce exactamente dentro del reenvío de Docker Desktop (proxy del host, vpnkit o el lado de la VM) **no está verificado**: haría falta capturar tráfico, y eso queda fuera de este step.

### 1.4 Veredicto por hipótesis

| Hipótesis | Veredicto | Celda que lo sustenta |
|---|---|---|
| H1 reenvío de puertos de Docker Desktop | **Ubicación confirmada (camino host a VM), mecanismo sin identificar.** Es ahí donde aparecen el modo lento y el coste de base | Modo lento en 17/32 celdas en ráfaga del host y 0/8 en el contenedor (`bimodal-ac`); `rtt` base 1,5–1,8 ms en el host frente a 0,7–0,8 ms en el contenedor (pasada 4) |
| H2 Nagle + ACK retardado (Nagle del cliente) | **Descartada** como causa de la discrepancia. Que la firma sea de ~40 ms sí apunta a un ACK retardado, pero dentro del camino del reenvío, no del Nagle del cliente. | `host-burst-nagleoff` lenta en 4/8 y `host-burst-l0-nagleoff` en 6/8 (`bimodal-ac`); `host-iso-nagleoff` 14,9 frente a 15,2 ms |
| H3 linger/batching | **Confirmada como componente del valor absoluto**, no de la diferencia aislado/ráfaga: el linger por defecto (5 ms) es casi toda la latencia de un mensaje en el modo rápido | `host-iso-linger0` 2,0 frente a 15,2 ms; `ctr-iso-linger0` 1,0 frente a 6,4 ms |
| H4 conexión o CPU en reposo | **Descartada** | `host-singles10-base` 14,6 frente a `host-iso-base` 15,2 ms; `ctr-singles10` 6,3 frente a 6,4 ms |
| H5 cadencia del bucle de poll | **Descartada** | `produce_to_ack` ≈ `int_latency` + `outbuf` + `rtt`, con menos de 1 ms de diferencia en todas las celdas con estadísticas |
| H6 resolución del temporizador de Windows (15,6 ms) | **Descartada tal como está formulada**: el sistema ya estaba a 1 ms y forzarlo no cambia nada. **Queda un efecto de Windows sin explicar (indeterminado):** el linger de 5 ms cuesta 12–13 ms de `int_latency` en el host frente a 5,2–5,3 ms en Linux. | `host-iso-timer1ms` 14,8 frente a 15,2 ms; `int_latency` host frente a contenedor (pasada 4) |

**La discrepancia del briefing:** el valor aislado del briefing no se reproduce, y el de la ráfaga es inestable.
- **Aislado:** en 4 pasadas, ninguna celda aislada superó 17 ms de p50 (bridge 14,6–16,9; probe en el host 15,1–16,6). Los "~42–44 ms con mensajes aislados" **no se reproducen**.
- **Ráfaga:** cae en ~16 ms o en ~55–59 ms según la conexión. Valores de 43–46 ms solo aparecen con `linger.ms=0` en el modo lento.
- **Lectura más probable:** las dos cifras del briefing son los dos modos de la ráfaga en el host, y no un efecto de la cadencia. Es una inferencia: las condiciones de la observación original no constan en el repo (sección 6).

### 1.5 Pasadas anteriores (datos crudos conservados)

Las pasadas 1 a 3 no son la referencia, pero se conservan etiquetadas en `results/` (`README.md` de esa carpeta). Coinciden con la pasada 4 en todo lo que llegaron a medir: aislado entre 14,6 y 16,9 ms, y la ráfaga bimodal (55–59 o 14–18 ms). La pasada 3 se hizo íntegramente en batería; ver desvíos en la sección 6.

## 2. Pregunta 2 — ubicación de clientes y relojes

### 2.1 Qué mediciones son de un solo proceso y cuáles cruzan procesos

| Tipo | Mediciones | Reloj que deben usar | Estado actual |
|---|---|---|---|
| Un solo proceso | `receive_to_produce_ms`, `produce_to_ack_ms` (bridge); `consume_to_alarm_ack_ms` (alarm consumer); espera del limitador del sink; el retraso del propio generador | monotónico | Ya monotónico (0.4) |
| Entre procesos | `event_to_receive_ms`, `event_to_ack_ms` (simulador → bridge); `event_to_alarm_ack_ms` (simulador → alarm consumer); toda cifra extremo a extremo del benchmark (F1, F2, F6) | reloj de pared común | Pared. En el host, Python usa `GetSystemTimePreciseAsFileTime` (1 µs). El consumer usa `performance.timeOrigin + now()`, fijado al arrancar, que diverge del reloj del sistema si este se ajusta durante la corrida; el consumer registra esa divergencia (`hr_minus_date_now_ms`). |

### 2.2 Desfase entre el host Windows y la VM de Docker

`clock_offset.py`: el host pregunta 200 veces por contenedor (dos contenedores intercalados, cada uno con su `docker run -i`). Calcula desfase = `CLOCK_REALTIME` del contenedor − punto medio de (t0, t1) del host, con una incertidumbre de ±RTT/2, y toma la mediana del 10 % de muestras con menor RTT (la regla de NTP). Datos en `results/clock-ac/`.

| Medición | Hora | Desfase VM − host | Incertidumbre | RTT mín / mediana | `CLOCK_MONOTONIC` VM − pared host |
|---|---|---|---|---|---|
| t0 | 22:23:50 | **+0,48 ms** (a 0,478, b 0,475) | ±0,46 ms | 0,8 / 1,0 ms | −1 791 333 344 189,5 ms |
| t10 | 22:33:58 | **+2,07 ms** (a 2,067, b 2,081) | ±0,47 ms | 0,7 / 1,0 ms | −1 791 333 344 125,1 ms |

- **Deriva del reloj de pared:** +1,6 ms en 10 minutos (unos 2,6 ppm), con dos medidas. No basta para caracterizarla a largo plazo (si se corrige a saltos o de forma gradual): **no verificado**.
- **`CLOCK_MONOTONIC` de la VM frente al reloj de pared del host:** se separan 64,5 ms en 10 minutos (unos 106 ppm). El reloj de pared de la VM se mantiene a ±2 ms del host porque algo lo disciplina, probablemente la sincronización de hora de Hyper-V (**no verificado**); el monotónico no. Por eso un intervalo monotónico medido en la VM no se puede comparar con instantes de pared del host.

### 2.3 Los contenedores comparten el reloj del kernel de la VM

- Los dos contenedores dan el mismo desfase con 0,003 ms (t0) y −0,014 ms (t10) de diferencia en `CLOCK_REALTIME`, y 0,003 / −0,015 ms en `CLOCK_MONOTONIC`, todo dentro de la incertidumbre de la medición.
- La fuente de reloj es la misma en el broker y en el contenedor de diagnóstico: `hyperv_clocksource_tsc_page`, con el kernel 6.6.87.2-microsoft-standard-WSL2.
- El contenedor está en el namespace de tiempo inicial (`/proc/self/ns/time` → `time:[4026531834]`), así que no tiene desplazamiento propio.

Verificado: comparten el reloj del kernel.

### 2.4 Dónde corren el generador de carga y el medidor

| | (A) Todo en contenedores dentro de la VM | (B) Clientes en el host (lo que hay hoy) |
|---|---|---|
| Qué mide | El pipeline: MQTT, bridge, Kafka y consumer, sobre la red interna de Docker | El pipeline más el reenvío de puertos de Docker Desktop (Windows → VM) en cada salto cliente ↔ broker |
| Reloj | Un solo reloj de kernel para generador, bridge y consumer (2.3): las cifras entre procesos no necesitan corrección | Un solo reloj de host (Windows) para generador, bridge y consumer: tampoco necesitan corrección, mientras ninguna marca de tiempo se tome en la VM |
| Sesgo | El generador y los clientes compiten con el broker por las 4 CPU de la VM | El reenvío añade una ida y vuelta de ~45 ms en una fracción aleatoria de las conexiones con ráfagas (1.3) y ~0,8 ms de RTT de base. El linger de 5 ms cuesta 12–13 ms en Windows frente a 5 ms en Linux (1.4, H6). La cifra quedaría dominada por un artefacto de Docker Desktop en Windows, no por el pipeline. |
| Coste | Contenerizar el bridge, el alarm consumer y el generador (Dockerfiles y un override de compose solo para el benchmark). Hoy corren en el host (`python -m …`, `node …`). Es trabajo de implementación fuera de este step. | Ninguno |

**Recomendación: (A)** para las cifras de pipeline (F1–F4). Con (B), F1 y F4 dependerían de en qué modo cae cada conexión.

Precaución: no mezclar las dos ubicaciones en una misma cifra. Una marca de pared del host frente a una de la VM llevaría el desfase medido (0,5–2 ms y variable), que es del orden de la propia latencia en el modo rápido.

### 2.5 Detectar que el generador compite por CPU

- **Retraso del generador.** Cada envío registra el instante programado y el real. Una corrida es inválida si el p99 del retraso supera, por ejemplo, 1 ms o el máximo supera 10 ms. Umbrales a decidir; ver D5. Es lo que ya hace `run_matrix.py` con 50 ms.
- **CPU por proceso y por contenedor, cada segundo.** Para los contenedores, `cpu.stat` del cgroup (o `docker stats`); en (B), el uso por proceso del host. Criterio propuesto: ningún proceso de cliente por encima del 80 % de un núcleo (el bridge Python es de un solo hilo para el procesado) y la VM por debajo del 80 % de sus 4 CPU. Por encima, se declara la corrida limitada por CPU.
- **Cuotas.** En (A), un override de compose solo para el benchmark que reserve CPU (`cpus:`) para el generador y para el broker. Así la competencia queda acotada y declarada. Coste: con 4 CPU, repartir cuotas reduce el throughput máximo alcanzable.
- **Comprobación cruzada.** Repetir un punto con el generador a la mitad de la tasa. Si la latencia del pipeline baja más de lo que explica la carga, el generador estaba interfiriendo.

## 3. Pregunta 3 — Kafka en tmpfs

### 3.1 Qué vive en tmpfs, y con qué límites

| Servicio | Rutas en tmpfs | Límite |
|---|---|---|
| broker | `/var/lib/kafka/data` (los logs de Kafka), `/etc/kafka/secrets` | ninguno en el compose (`docker-compose.yml:94-96`): el tmpfs toma el defecto del kernel, la mitad de la RAM de la VM, **6 272 180 224 B (5,84 GiB)** (`df -B1` dentro del broker) |
| zookeeper | `/var/lib/zookeeper/data`, `/var/lib/zookeeper/log`, `/etc/zookeeper/secrets` | igual (`:58-61`) |
| schema-registry | `/etc/schema-registry/secrets` | igual |
| mosquitto | `/mosquitto/data`, `/mosquitto/log` (persistencia desactivada, quedan vacías) | igual |

Ningún servicio tiene límite de memoria ni de CPU (`docker inspect`: `Memory=0`, `NanoCpus=0`). El tmpfs ocupa RAM de la VM, la misma que usan las JVM. En reposo, con el stack recién arrancado: broker 1,03 GiB, schema-registry 259 MiB, zookeeper 104 MiB (`docker stats`), de los 11,68 GiB de la VM.

Política de flush del broker (`kafka-configs --describe --all`):
- `log.flush.interval.messages` y `log.flush.scheduler.interval.ms` = 9223372036854775807; `log.flush.interval.ms` = null. Kafka nunca fuerza fsync: escribe en la page cache y el kernel decide cuándo volcar.
- `log.retention.hours` = 168, `log.retention.bytes` = −1, `log.segment.bytes` = 1 GiB. `telemetry.raw` no tiene configuración propia (`config/topics.yaml`). Durante una corrida no se borra nada.

### 3.2 Volumen de datos de una corrida

Medido en el log, a tasa baja:
- `telemetry.raw`: 440 registros en 54 608 B, **124,1 B por registro**. Valor Avro de 65–77 B, clave de 7–13 B, más la cabecera de registro y de batch.
- `bench.diag.produce-ack`: 7 260 registros en 899 441 B, 123,9 B por registro.

Los índices (`.index` y `.timeindex`, 10 MiB aparentes cada uno por segmento) son dispersos: ocupan 0–4 KiB reales (`du` aparente frente a asignado). A tasa alta, librdkafka agrupa más registros por batch y la cabecera de 61 B se reparte entre ellos; se estiman ~90–100 B por registro (**no medido**).

Con 124 B por registro (cota superior), en una corrida de 10 minutos (5 medidos, más calentamiento y margen):

| Tasa | Por segundo | 10 min | Fracción del tmpfs (5,84 GiB) | Minutos hasta llenarlo |
|---|---|---|---|---|
| 5 msg/s (en vivo, 5 activos) | 0,6 KB | 0,37 MB | ~0 % | — |
| 1 000 msg/s | 124 KB | 74 MB | 1,2 % | ~840 |
| 5 000 msg/s | 620 KB | 372 MB | 6 % | ~170 |
| 10 000 msg/s | 1,24 MB | 744 MB | 12 % | ~84 |
| 20 000 msg/s | 2,48 MB | 1,49 GB | 24 % | ~42 |

- Con el stack recreado entre corridas (5.1.5), el tmpfs empieza vacío cada vez y las repeticiones no se acumulan. Sin recrearlo, cinco repeticiones a 10 000 msg/s ocuparían 3,7 GB: el 59 % del tmpfs y un tercio de la RAM de la VM, compitiendo con los heaps.
- Los 12 GB de WSL son el techo de todo junto (tmpfs, JVM, page cache y clientes en contenedor, si se elige 2.4 A). El límite práctico lo pone la memoria antes que el tamaño del tmpfs.
- `telemetry.alarms` (compactado) y `telemetry.dlq` (esperado 0) son despreciables frente a `telemetry.raw`.
- **Hallazgo:** con los 5 `asset_id` actuales, el particionador por defecto reparte las claves solo entre las particiones 0 y 1 de `telemetry.raw`; la 2 recibe 0 registros (offsets finales 352, 88 y 0, con pump-01 entero en la 0). Un benchmark de throughput con 5 activos usa 2 de las 3 particiones. Se declara, o se usan más activos; ver D6.

### 3.3 Opciones

| Opción | Qué es | Coste |
|---|---|---|
| (A) Mantener tmpfs y declararlo | Benchmark sobre el compose tal como está; la cifra lleva "almacenamiento del broker en RAM (tmpfs)" | Ninguno de implementación. La cifra no incluye ninguna escritura a disco, ni siquiera diferida, y es optimista frente a un despliegue con disco. |
| (B) Volumen con nombre solo para el benchmark | Un override de compose (`docker-compose.bench.yml`) monta un volumen en `/var/lib/kafka/data`; CI y el uso normal siguen en tmpfs | El override, y decidir qué pasa con ZooKeeper (ver 3.4). El volumen vive en el VHDX de Docker Desktop, que crece y no se encoge solo. Hay que vaciarlo entre corridas (`down` sin `-v` no lo borra; borrar un volumen requiere tu autorización según las reglas del proyecto). |
| (C) Medir ambas y reportar la diferencia | (A) y (B) en el mismo punto de carga | El doble de corridas en ese punto. Da la evidencia de cuánto pesa el tmpfs en vez de suponerlo. |

### 3.4 Qué cambia de verdad con (B)

- **Latencia del productor: probablemente poco.** Kafka nunca hace fsync por mensaje (3.1). Con tmpfs o con volumen, `produce()` termina en cuanto el registro está en la page cache. El ack (acks=all, una réplica) no espera al disco en ninguno de los dos casos.
- **Escritura diferida.** Con volumen, el kernel de la VM vuelca la page cache al ext4 del VHDX en segundo plano (expiración por defecto de las páginas sucias: 30 s). Esa E/S pasa por el disco virtual sobre NTFS y puede competir con el resto a tasas altas: es el único efecto real sobre la latencia, y sería intermitente. **No medido**; es lo que mediría (C).
- **Memoria.** La page cache de un volumen es recuperable; las páginas de tmpfs no, salvo a swap. Con (B) el límite práctico de una corrida larga deja de ser la RAM.
- **Persistencia.** (B) haría que Kafka sobreviviera a un reinicio, que es la opción B del backlog (alarmas fantasma tras un reinicio del broker). Pero si Kafka persiste y ZooKeeper sigue en tmpfs, el broker arranca contra un ZooKeeper vacío con otro cluster id y falla. Es el problema de estado obsoleto que llevó a usar tmpfs en la Fase 0 (`docker-compose.yml:53-57`). Persistir solo para el benchmark no lo necesita: el stack se recrea por corrida.
- **Recomendación: (A) declarado para las cifras publicadas, más una única comprobación (C) en el punto de F1/F3** para poder escribir en el README cuánto cambia, o que no cambia, con disco. La decisión sobre la persistencia de producción (backlog, opción B) es otra y no debería resolverse dentro del benchmark (D4).

## 4. Pregunta 4 — camino a ThingsBoard

### 4.1 Valores actuales, leídos del código y la config

| Parámetro | Valor | Dónde |
|---|---|---|
| Límite del sink por activo | 10 msg/s, telemetría y alarmas juntas; token bucket de un segundo | `config/thingsboard.yaml:20`; `AssetRateLimiter`, `processing/thingsboard_sink/__main__.py:108-` |
| Retardos de batch SQL | `SQL_TS_BATCH_MAX_DELAY_MS`, `SQL_TS_LATEST_BATCH_MAX_DELAY_MS` y `SQL_ATTRIBUTES_BATCH_MAX_DELAY_MS` = 25 ms | `docker-compose.yml:150-152` |
| Cola de reglas | PipelineSequential: `SEQUENTIAL_BY_ORIGINATOR` con batch de 100, `packProcessingTimeout` 60 s, `RETRY_FAILED_AND_TIMED_OUT` con 20 reintentos de 5 a 60 s | `scripts/thingsboard/import_rule_chains.py:106-113` |
| Colas de ThingsBoard | en memoria (`TB_QUEUE_TYPE: in-memory`) | `docker-compose.yml:131` |
| Heap | `-Xmx1g` | `docker-compose.yml:137` |
| Entrega del sink | QoS 1; el offset se guarda tras el PUBACK | docstring del sink, líneas 38-45 |

Coinciden con lo que dice el backlog: 10 msg/s, 25 ms y el acoplamiento entre ambos (`docs/internal/phase3_backlog.md`, sección 3.4). Las capacidades de 20,2, 6,9 y 43,1 msg/s son mediciones del 2026-10-04 que este step **no repitió**.

### 4.2 Frontera entre pipeline y sink

- **El pipeline termina en el ack de Kafka de la escritura en `telemetry.alarms`** (alarmas) y **en el ack de `telemetry.raw`** (telemetría). Es la propuesta del briefing, precisada: "alarma confirmada" significa que se resolvió el `await producer.send` (acks -1). Ese es el instante que ya registra `alarm_ack_ts_ms`; el benchmark no necesita leer el topic para obtenerlo.
- **El sink empieza cuando el consumidor del sink lee el registro** de `telemetry.raw` o de `telemetry.alarms`, en su propio grupo de consumo. Su lag forma parte de la medición del sink, no de la del pipeline.

### 4.3 Perfiles

Recomendación: el pipeline se mide en perfil **core** y el sink en perfil **full**. ThingsBoard con Postgres reserva 1 GB de heap y consume CPU de forma continua (GC, colas, estadísticas). En la misma VM de 4 CPU compite con Kafka y con los clientes; medir el pipeline con ThingsBoard arrancado mezclaría sus costes. Coste: dos sesiones de medición en lugar de una, más el arranque del perfil full (minutos en el primer arranque).

### 4.4 Qué se mide en el sink y cómo se observa

| Qué | Definición | Cómo se observa |
|---|---|---|
| Tasa sostenida | mensajes por segundo por activo que ThingsBoard procesa sin crecer el retraso, con el límite de 10 msg/s | Lag del grupo del sink (offsets comprometidos frente a high watermark, muestreados cada segundo) más los contadores del sink (`telemetry_forwarded`, `alarm_events_forwarded`) |
| Latencia hasta ThingsBoard | `ts` de la muestra (o instante programado del generador) → el valor es visible en ThingsBoard | Propuesta, **no verificada**: suscripción WebSocket de ThingsBoard (telemetría del dispositivo) con el instante de llegada del cliente medido con el mismo reloj del host. Mide "guardado y notificado", no solo "aceptado por MQTT". El PUBACK del sink da una segunda marca (transporte aceptado), pero el sink no la registra hoy: haría falta instrumentarlo (decisión D7). |
| Comportamiento por encima del techo | generador a más de 10 msg/s por activo durante un tiempo fijo, y luego a cero | Retraso acumulado = lag del sink en el tiempo; tiempo de drenaje; esperado ≈ backlog / (10 × activos), con el backlog repartido por activo |
| Pérdidas = 0, verificable | todo registro escrito en Kafka llega como valor a ThingsBoard | Cuenta final por dispositivo: número de puntos de serie temporal en ThingsBoard (REST, ventana de la corrida) = registros del activo en `telemetry.raw` en esa ventana. Más las estadísticas de la cola (fallidos = 0, como ya hacen las pruebas de integración con `queue_stats_since`) y `alarm_mapping_failed` = 0. |

El backlog ya midió un caso: 3 200 mensajes de un activo en 320 s (`test_a_backlog_of_one_asset_reaches_thingsboard_without_a_drop`, `tests/integration/test_thingsboard_delivery.py:146`). Es una prueba funcional, no un benchmark.

## 5. Pregunta 5 — cifras candidatas a publicación

### 5.1 Reglas del protocolo (aplican a todas las cifras)

1. **Lazo abierto a tasa fija.** El generador programa cada envío en `t0 + i / tasa` sobre un reloj monotónico y no espera respuestas. La latencia se mide **desde el instante programado**, no desde el envío real. Así, si el generador se retrasa, ese retraso cuenta como latencia y se evita la omisión coordinada.
   - El simulador actual no sirve como generador. Toma `ts` en el envío real (`utc_now_iso()`, `edge/simulator/__main__.py:238`) y, si se retrasa, se pone al día sin dormir (`:251-253`). Ese retraso no se vería.
   - Propuesta: un generador dedicado (`scripts/bench/`) que estampe `ts` = instante programado (es la hora de la muestra, legítima en el esquema) y registre aparte el envío real, para medir su propio retraso.
2. **Entorno controlado.** En corriente alterna (comprobada y registrada por corrida), con el mismo plan de energía, sin contenedores ajenos corriendo (la comprobación de `docker ps` de `run_matrix.py`) y con la máquina sin otra carga. El entorno se registra entero en cada archivo de resultados (sección 0.3).
3. **Calentamiento descartado.** Los primeros 30 s de cada corrida no cuentan (propuesta). La comparación del primer segundo con el resto ya se ve en las celdas de la sección 1, donde se descartaron 20 mensajes.
4. **Duración mínima por corrida: 5 minutos medidos** (más el calentamiento). Con eso hay al menos 1 500 muestras a 5 msg/s, y del orden de 10⁵ a 10⁶ a tasas altas.
5. **Repeticiones: 5 corridas por punto**, con el stack recreado entre corridas (`make down` y `make up-core`, tmpfs vacío). Se publica la mediana de las 5 y el rango min–max entre corridas.
6. **Percentiles sobre muestras crudas.** Por corrida, sobre todas las muestras crudas del intervalo medido, nunca promediando percentiles. p50, p95 y p99. **p99 solo si la corrida tiene ≥ 10 000 muestras** (al menos 100 por encima del p99) y p99,9 solo con ≥ 100 000. En caso contrario no se publica.
7. **Throughput máximo sostenible.** Es la tasa más alta a la que, durante los 5 minutos medidos de las 5 repeticiones, se cumplen las tres condiciones siguientes. Se busca subiendo la tasa por escalones.
   - (a) lag del consumidor acotado: la pendiente de una regresión lineal del lag frente al tiempo es ≤ 0, y el lag máximo es < 1 s de mensajes;
   - (b) latencia acotada: el p99 extremo a extremo es < 1 s (umbral a decidir, D8) y no crece de la primera mitad a la segunda;
   - (c) cero pérdidas (punto 8).
8. **Integridad por corrida:**
   - enviados por el generador = recibidos por el bridge = registros nuevos en `telemetry.raw` (watermarks antes y después);
   - DLQ contada (registros nuevos en `telemetry.dlq`, esperado 0);
   - alarmas esperadas frente a emitidas, si la carga las provoca.
   
   Una corrida que no cuadra se invalida y no se promedia.
9. **Resultados versionados.** Cada corrida escribe un archivo legible por máquina en `bench/results/<fecha>-<id>/`: el entorno, los parámetros, las muestras crudas comprimidas o un histograma HDR de resolución fija, los contadores de integridad y el estado de energía. Las cifras del README se generan con un script a partir de esos archivos, nunca a mano.
10. **Declaración del entorno junto a toda cifra publicada:** una sola máquina (portátil i7-10750H), Docker Desktop sobre WSL 2 con 4 CPU y 12 GB, un broker Kafka sin replicación, almacenamiento en tmpfs (o lo que decida D4), datos sintéticos. **No es una cifra de capacidad de producción.**

### 5.2 Cifras candidatas

| # | Cifra | Definición exacta | Puntos de medición | Unidad | Limitación que debe acompañarla |
|---|---|---|---|---|---|
| F1 | Latencia del bridge | `ts` programado → ack de Kafka de `telemetry.raw` | Generador (instante programado, pared) y `receive_ts_ms` + `event_to_ack_ms` del bridge; mismo reloj de host si generador y bridge están en el mismo sitio (sección 2) | ms, p50/p95/p99 a una tasa fija declarada | Incluye MQTT y la CPU compartida; en tmpfs; un broker sin réplicas (acks=all equivale a 1 réplica) |
| F2 | Latencia extremo a extremo de alarma | `ts` programado de la muestra que dispara → ack de la alarma en `telemetry.alarms` (`alarm_ack_ts_ms`) | Generador y alarm consumer; cruza procesos y lenguajes: requiere el mismo reloj de pared (sección 2) | ms, p50/p95; p99 solo con muestras suficientes | Las alarmas son escasas por diseño (persistencia en muestras): o se provocan muchas con un patrón de fallos programado, o se publica un p95 con N declarado |
| F3 | Throughput máximo sostenible del pipeline | Criterio 5.1.7 sobre bridge + alarm consumer | Generador, lag de los grupos, contadores de integridad | msg/s (total y por activo) | Lo limita el cliente Python de un solo hilo del bridge y la CPU compartida, no Kafka; declarar que es la cifra de este pipeline, no la de Kafka |
| F4 | Coste de la escritura a Kafka | `produce_to_ack_ms` del bridge a la tasa de F1 | Bridge (monotónico) | ms p50/p95 | Muy sensible al entorno: depende de dónde corre el cliente y de `linger.ms` (sección 1). Publicable solo con esa explicación, o mejor no publicarla sola |
| F5 | Techo del camino a ThingsBoard | Tasa sostenida por activo con el límite de 10 msg/s y ThingsBoard sin perder | Sección 4.4 | msg/s por activo | Perfil full; acoplado a los retardos SQL de 25 ms; cifra separada del pipeline (backlog de Fase 3) |
| F6 | Latencia hasta ThingsBoard | `ts` programado → valor visible en ThingsBoard (WebSocket) | Sección 4.4 | ms p50/p95 | Incluye los retardos de batch SQL; perfil full; método no verificado (D7) |
| F7 | Tiempo de drenaje tras una caída de ThingsBoard | Backlog de N mensajes → lag 0 | Lag del sink | s | Lineal en el backlog y dependiente de cómo se reparte entre activos (backlog de Fase 3, 3.4) |

Con cada cifra van siempre la tasa de entrada y el número de muestras. Las cifras F1 a F3 son las candidatas principales para el README; F4 es un dato interno; F5 a F7 van en una sección aparte.

## 6. Desvíos respecto al briefing

1. **No existe `src/`.** El código está en `edge/`, `bridge/`, `processing/`, `broker/` y `common/`. No se modificó ninguno de ellos, ni `config/`, ni `docker-compose.yml`.
2. **`ts_to_ack_ms` ya no existe:** se eliminó en `fe37d8f` (Task 3.1-A).
3. **El origen de "~16 ms en el smoke, ~42–44 ms con mensajes aislados" no está en el repo.** No aparece en ningún documento ni mensaje de commit (`git log --all` con esas cifras). En STEP 0 de la Task 3.1, el commit `fe37d8f` midió p50 de 56–64 ms con el bridge. Los ~42–44 ms aislados no se reproducen (1.4).
4. **No hay contenedores de Airflow ni de Hadoop**: Anthony los bajó el 2026-10-05 con `docker compose -p <proyecto> down`, sin `-v`, y sus volúmenes se conservan (0.2).
5. **Criterio de validez.** Exigir corriente alterna fue idea del agente ejecutor (Claude Code), añadida tras la primera pasada (antes no había ningún criterio). En la pasada 3, con el portátil en batería, el agente lo cambió por su cuenta a "la misma fuente de alimentación durante toda la pasada, batería incluida". Eso se apartó del criterio del owner ("en corriente alterna durante toda la pasada"), que el owner restauró. La pasada 3 se paró a indicación del owner y sus datos se conservan, etiquetados como batería (`results/run3-battery-stopped`).
6. **Cuatro pasadas en vez de una:**
   - pasada 1: interrumpida por una suspensión al cerrar la tapa y luego una hibernación, de 22:50 a 21:03 del día siguiente (registro de sistema, Kernel-Power 42 y 107); dos celdas inválidas;
   - pasada 2: desenchufada a mitad de una celda (Kernel-Power 105, 21:11:48);
   - pasada 3: en batería;
   - pasada 4: la de referencia.
   
   Todas conservadas (`results/README.md`).
7. **Experimento añadido a la matriz: `bimodal.py`**, 40 celdas cortas en ráfaga. Hizo falta porque una celda por variante no basta cuando la ráfaga es bimodal. Es un experimento de diagnóstico de la sección 1; lo señalo porque no estaba en la matriz que pide el briefing.
8. **Inspecciones de solo lectura fuera de las secciones 1 y 2**, para la sección 3: `df` y `du` dentro del broker, `kafka-configs --describe --all`, `GetOffsetShell` y `docker stats`. No escriben nada, pero el briefing solo permitía ejecutar experimentos de la sección 1 y relojes de la sección 2, así que lo declaro.
9. **Efectos laterales en Docker:**
   - descargué la imagen `python:3.13.4-slim-bookworm` y construí `bench-diag:local` (`scripts/bench/diagnostics/Dockerfile`), ambas locales;
   - creé el topic scratch `bench.diag.produce-ack` en el broker (vive en tmpfs y desaparece con `make down`);
   - recreé el stack core tres veces (`make down` y `make up-core`), nunca con `-v`;
   - ningún contenedor ajeno corrió durante las mediciones: lo comprobé antes de cada celda.
10. **Sección 4:** no se ejecutó nada contra ThingsBoard; todo sale de la lectura del código y de la config.

## 7. Lo que quedó sin verificar

- **Los valores por defecto de librdkafka** (`linger.ms` 5, Nagle activo, sin compresión, batch): son los documentados; la librería solo imprime lo que no es por defecto (0.3). El efecto de `linger0` confirma indirectamente que el linger no es 0.
- **El Nagle por defecto de kafkajs/Node** (`noDelay: false`): leído en el código y en la documentación, no medido.
- **El mecanismo exacto del modo lento** (dónde se produce el ACK retardado dentro del reenvío de Docker Desktop) y **por qué el linger de 5 ms cuesta 12–13 ms en Windows**: haría falta capturar tráfico o trazar librdkafka.
- **Por qué solo las ráfagas** caen en el modo lento y los mensajes aislados nunca.
- **La deriva del reloj a largo plazo**, y si la VM se sincroniza con Hyper-V a saltos o de forma gradual: solo hay dos medidas a 10 minutos.
- **El tamaño por registro a tasa alta** (~90–100 B estimados) y el efecto de la escritura diferida a disco con la opción (B) de la sección 3.
- **La latencia hasta ThingsBoard por WebSocket** (4.4) y si el bridge y el consumer aceptan activos que no están en `config/telemetry.yaml` (D6).
- **Las capacidades de ThingsBoard** de 6,9, 20,2 y 43,1 msg/s: son del 2026-10-04 y no se repitieron.
- **El origen de la observación de 16 frente a 42–44 ms** (6.3).

## Decisiones que necesita Anthony

| # | Decisión | Opciones y coste | Recomendación |
|---|---|---|---|
| D1 | Dónde corren el generador, el bridge y el consumer durante el benchmark | (A) todos en contenedores en la VM: Dockerfiles y un override de compose para el benchmark, una tarea de implementación. (B) en el host, como hoy: sin coste, pero las cifras incluyen el artefacto de reenvío (modo de ~45 ms al azar por conexión en ráfagas, +0,8 ms de RTT, linger de 5 ms que cuesta 12–13 ms). (C) los dos: el doble de corridas. | **(A)** para F1–F4; se puede añadir una corrida (B) para ilustrar en el README qué cambia en Windows. |
| D2 | `linger.ms` del bridge | (a) dejar el defecto (5 ms) y declararlo: sin coste; el linger domina la latencia de un mensaje (1.4, H3). (b) `linger.ms=0` en el bridge: un cambio de config en código fuera de este step; baja la latencia por mensaje (de 6,4 a 1,0 ms en el contenedor) a costa de más peticiones y menos throughput por petición. (c) medir ambos en el benchmark: más corridas. | **(a)** para el benchmark, más una nota con la cifra de `linger0`. Cambiar el bridge es una decisión de producto, no del benchmark. |
| D3 | Generador de carga | (a) uno nuevo en `scripts/bench/`, en lazo abierto, que estampe `ts` = instante programado y registre el envío real: implementación nueva. (b) adaptar el simulador: toca `edge/`, y su `ts` es el del envío real (5.1.1). | **(a)**. |
| D4 | Almacenamiento de Kafka para el benchmark | (A) tmpfs declarado: sin coste. (B) volumen solo para el benchmark: un override, y vaciar el volumen entre corridas (borrarlo requiere tu autorización). (C) ambos en un punto: el doble de corridas en ese punto. | **(A) más una comprobación (C)** en el punto de F1/F3. La persistencia de producción (opción B del backlog) se decide aparte. |
| D5 | Parámetros de corrida | Propuestos en 5.1: corriente alterna y tapa abierta; calentamiento de 30 s; 5 min medidos; 5 repeticiones con el stack recreado; p99 solo con ≥ 10 000 muestras; corrida inválida si el retraso del generador supera p99 1 ms o máximo 10 ms, o si algún cliente pasa del 80 % de un núcleo. Coste: con 5 repeticiones de unos 7 minutos cada una más el arranque, cada punto de carga lleva ~45 minutos. | Los propuestos; tú fijas los umbrales. |
| D6 | Activos y particiones | (a) los 5 activos reales: solo 2 de las 3 particiones de `telemetry.raw` reciben datos (3.2); declararlo. (b) un generador con N activos sintéticos (por ejemplo, 30) para el throughput: hay que verificar si el bridge y el consumer aceptan activos que no están en la config (no verificado). | **(a) para latencia, (b) para throughput** si se verifica que no hace falta tocar el código. |
| D7 | Latencia hasta ThingsBoard (F6) | (a) observador por WebSocket: un script nuevo, el método no está verificado. (b) instrumentar el sink para que registre el PUBACK: cambio de código; mide "aceptado por el transporte", no "procesado". (c) no publicar F6: solo F5 y F7. | **(a)**, verificado en el STEP de implementación; si no funciona, **(c)**. |
| D8 | Criterio de throughput máximo sostenible | Umbrales de 5.1.7: pendiente del lag ≤ 0 y lag máximo < 1 s de mensajes; p99 extremo a extremo < 1 s y sin crecer de la primera mitad a la segunda; cero pérdidas. Coste: con umbrales más estrictos, la tasa publicada sale más baja. | Los propuestos. |
| D9 | Perfiles | Pipeline en perfil core; sink en perfil full (4.3). Coste: dos sesiones de medición. | Sí. |

## Enmiendas del 2026-10-09

Anthony confirmó el 2026-10-09 la aprobación del DETENTE 3.3.a con D1–D9 según la recomendación, con las enmiendas y la decisión siguientes. Fuente: `docs/internal/claude_code_briefings/phase3_task3.3_step1_benchmark_implementation.md`. Donde difieran del resto del documento, prevalecen estas.

- **E1 (sobre D1).** El bridge, el alarm consumer y el sink pasan a correr en contenedores, como un perfil del compose. Es un cambio de producto, no un apaño del benchmark: tiene que servirle a cualquiera que clone el repo. Las CPU de la VM se fijan y se declaran antes de medir.
- **E2 (sobre D5).** El techo de throughput se busca con una corrida por escalón. Las cinco repeticiones se hacen solo en el escalón candidato y en el siguiente.
- **E3 (sobre D6 y D7).** Antes de construir nada se verifica (a) si el bridge y el alarm consumer aceptan activos que no están en la config, y (b) si funciona el observador por WebSocket de ThingsBoard. Si (b) falla, se elimina la cifra F6 (latencia hasta ThingsBoard) sin improvisar otro método.
- **E4 (texto).** Aplicada en 0.2, en 1.4 (H1) y en 6.4.
- **Decisión D8.** El criterio de throughput máximo sostenible es un p99 extremo a extremo menor de 1 s (justificado como un periodo de muestreo del sensor), con lag acotado y cero pérdidas. Es la única cifra de throughput que se publica. Los resultados crudos deben permitir recalcular el corte con otros umbrales sin repetir corridas. Sustituye a la fila D8 de la tabla de decisiones y a los umbrales de 5.1.7 donde difieran.
- **D2 (precisión).** El bridge conserva el `linger.ms` por defecto. El arnés tiene que poder lanzar una corrida con otro valor por parámetro, sin tocar el código del bridge, para una posible corrida de sensibilidad que se decidirá más adelante.
