# Task 3.3: informe de cierre (benchmark del pipeline)

Estado: preparado para revisión. Rama `feature/phase3-benchmark`, sin merge a `main`, sin tag. El README no se ha escrito.

Las cifras de la sección 1 son la salida de `make bench-report` (`python -m bench.report`) en el commit `b538247`, copiada tal cual por un script; ninguna se escribió a mano. Las de las demás secciones citan su fuente: un commit, un documento de `docs/internal/` o un resultado de `bench/results/`.

## 1. Cifras F1 a F4 y F3 (configuración final)

Comando: `make bench-report`. Los percentiles van en milisegundos enteros redondeados hacia arriba, con la mediana y el rango entre repeticiones; "n per run" es el número de muestras de cada corrida.

### Final configuration (DETENTE 3.3.i)

The tables below use only the 23 runs on the final configuration (harness revision "final configuration (after 3.3.h and 3.3.i)"), made at commit(s) 8d5abfc, d53be71, e18d48e, eff697c, none with uncommitted changes. It differs from the earlier runs in Mosquitto's queue per client (60000 messages / 25 MiB instead of about 1040), the bridge's drop monitor, the broker and ZooKeeper healthchecks (no JVM; every earlier run had one running inside the broker's pinned vCPUs every ~11 s) and the cp-server telemetry reporter and Self-Balancing turned off. A run on it is valid only if Mosquitto dropped no message.

Earlier runs, kept as history in the Runs table and not in the tables below: latency 5/s: 5; latency 50/s: 5; throughput 250/s: 1; throughput 500/s: 1; throughput 1000/s: 1; throughput 2000/s: 8; throughput 3000/s: 2; throughput (diagnostic) 3000/s: 2.

### Repetitions (median and range across valid runs, whole ms)

| Mode | Rate/s | Bridge log | Revision | Runs | Figure | p50 | p95 | p99 | n per run |
|---|---|---|---|---|---|---|---|---|---|
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 8 (8-8) | 13 (12-13) | - | 1500 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | ingest | 2 (2-2) | 4 (4-4) | - | 1500 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | produce_to_visible | 7 (7-7) | 10 (10-10) | - | 1500 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F2 alarm | 34 (34-39) | 55 (48-58) | - | 300 |
| latency | 5 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F4 produce-to-ack | 7 (7-7) | 9 (8-9) | - | 1500 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 7 (7-8) | 11 (11-12) | 13 (12-13) | 15000 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | ingest | 1 (1-1) | 4 (4-4) | 4 (4-4) | 15000 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | produce_to_visible | 7 (7-7) | 9 (9-9) | 10 (10-10) | 15000 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F2 alarm | 35 (34-36) | 55 (52-56) | - | 300 |
| latency | 50 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F4 produce-to-ack | 6 (6-6) | 8 (8-8) | 8 (8-8) | 15000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F1 e2e | 6 (6-6) | 11 (10-16) | 26 (19-59) | 600000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | ingest | 2 (2-2) | 5 (4-8) | 18 (11-52) | 600000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | produce_to_visible | 5 (5-5) | 8 (8-8) | 10 (9-11) | 600000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F2 alarm | 28 (24-30) | 46 (37-48) | - | 6000 |
| throughput | 2000 | INFO | final configuration (after 3.3.h and 3.3.i) | 5 | F4 produce-to-ack | 5 (5-5) | 8 (8-10) | 18 (12-28) | 600000-600001 |

### Throughput ladder (F3, D8; bridge at the product default log level)

- 250/s: sustainable
- 500/s: sustainable
- 1000/s: sustainable
- 2000/s: sustainable
- 3000/s: not sustainable - CPU-bound instrument(s): generator at 94 % of a core

Candidate: 2000/s; runs 5 of the 5 E2 needs; confirmed.

E2 attempts (6, all listed; an attempt invalid only because an instrument was at its limit is replaced by its valid repeat, owner DETENTE 3.3.g):
- 20261012T172558Z-throughput-2000-r1 (r1): valid, sustainable
- 20261012T175245Z-throughput-2000-r2 (r2): valid, sustainable
- 20261012T180130Z-throughput-2000-r3 (r3): invalid because of the instrument (CPU-bound instrument(s): generator at 102 % of a core; generator saturated (lateness p99 2.0 ms, max 272.2129821777344 ms)); replaced by its repeat 20261012T181010Z-throughput-2000-r3; bridge CPU peak 105 % of a core, Mosquitto dropped 0
- 20261012T181010Z-throughput-2000-r3 (r3): valid, sustainable
- 20261012T181850Z-throughput-2000-r4 (r4): valid, sustainable
- 20261012T182731Z-throughput-2000-r5 (r5): valid, sustainable
Limited by the measuring tools: the step above the candidate, 3000/s, could not be measured validly (an instrument at its limit); the ladder ends there, with no intermediate step.

### Log-level sensitivity (same steps, bridge at the sensitivity log level)

| Rate/s | Log level | Runs | Sustainable | F1 e2e p50/p95/p99 ms |
|---|---|---|---|---|
| 2000 | INFO | 5 | 5 of 5 | 6 / 12 / 29 (n=3000000) |
| 2000 | WARNING | 1 | 1 of 1 | 5 / 8 / 12 (n=600000) |

The product-default side of each row uses the runs E2 counts at that step (an attempt replaced by its repeat is left out, as in E2; owner, DETENTE 3.3.g).
The WARNING side at 2000/s is a single run.

**Enunciado de F3, tal como lo imprime el informe:** "Candidate: 2000/s; runs 5 of the 5 E2 needs; confirmed.", junto con "Limited by the measuring tools: the step above the candidate, 3000/s, could not be measured validly (an instrument at its limit); the ladder ends there, with no intermediate step." El mismo corte con p99 < 100 ms (`make bench-report` con `--p99-ms 100`, cifra interna) da el mismo candidato, 2000/s.

**Corridas.** La configuración final reúne 23 corridas con resultados: latencia 10 (todas válidas), escalera y E2 11 (8 válidas, 3 inválidas: 3000/s dos veces y el primer intento de r3 a 2000/s, todas por un instrumento en su límite) y el bloque WARNING 2 (2000/s válida; el diagnóstico a 3000/s inválido, con el generador al 93 %). Hay además 2 intentos WARNING sin resultados (fallo del arnés, sección 2.6). Cada corrida de la configuración final exige que Mosquitto no descarte ningún mensaje, y en ninguna descartó.

## 2. Hallazgos (problema, acción, resultado)

### 2.1 El artefacto de latencia entre el host y la VM (STEP 0)
- **Problema.** El briefing citaba unos 16 ms en el smoke y unos 42-44 ms con mensajes aislados para el bridge en el host de Windows. Las cifras no se podían usar sin saber de dónde salían.
- **Acción.** Matriz de diagnóstico en cuatro pasadas y un experimento añadido (`bimodal.py`, 40 celdas en ráfaga) para separar seis hipótesis (`docs/internal/benchmark_protocol_proposal.md`, 1.3 y 1.4).
- **Resultado.** Las ráfagas desde el host caen, según la conexión, en un modo lento con una ida y vuelta de unos 45 ms: 17 de 32 celdas en ráfaga en el host y 0 de 8 celdas en ráfaga en el contenedor, las dos en el mismo experimento (`bimodal-ac`; protocolo, tablas de 1.3 y 1.4). El "0 de 13" del informe del STEP 0 cuenta todas las celdas del contenedor, también las 5 de la pasada 4, que no son todas en ráfaga (protocolo, 1.3: "5 en la pasada 4 y 8 en bimodal-ac"). Los mensajes aislados quedaron entre 14,6 y 16,9 ms de p50; los 42-44 ms no se reprodujeron. La ubicación está confirmada (el camino del host a la VM del reenvío de Docker Desktop); el mecanismo no está identificado. Decisión D1, opción (A), con la enmienda E1 (el bridge, el consumer y el sink en contenedores, como perfil del compose): todo el benchmark corre en contenedores dentro de la VM, de modo que ninguna cifra cruza ese camino.

### 2.2 StopTimeout de Docker Desktop
- **Problema.** Docker Desktop 4.53 creó los contenedores del proyecto, y los de cualquier otro, con `Config.StopTimeout` de 1 s (`docker inspect`, 2026-10-09). Ningún fichero del repositorio lo fija, y el valor por defecto del motor es 10 s. El alarm consumer necesitaba de 1,5 a 3,6 s para desconectarse y Docker lo mataba a mitad de la parada; el bridge vacía su productor hasta 10 s. En el CI de Linux la misma parada espera 10 s, así que la parada se comportaba distinto en los dos sitios.
- **Origen, identificado en el DETENTE 3.3.c (2026-10-10; mensaje de `da7a728`):** el proxy de la API de Docker Desktop. Se mostró enviando la misma petición de creación de contenedor, sin StopTimeout, desde un contenedor de prueba a los dos sockets de la VM de Docker Desktop:
  - por `docker.raw.sock` (el propio dockerd), el contenedor quedó con StopTimeout nulo, el valor por defecto del motor;
  - por `docker.sock` (el proxy, el que usan la CLI y Compose), quedó con 1;
  - un `docker run` simple y un proyecto compose mínimo fuera del repositorio también recibieron 1, y `--stop-timeout 7` se respetó.

  Por qué el proxy elige 1 no está identificado.
- **Acción.**
  - `stop_grace_period: 30s` en los contenedores del pipeline (`7386596`);
  - un valor explícito para cada servicio (`da7a728`): broker 30 s; ZooKeeper y Schema Registry 10 s; Mosquitto 5 s; ThingsBoard 5 s (sección 2.3).
- **Resultado.**
  - Verificado tras `make up` (perfil full) y `make down`: Mosquitto salió con 0, y Schema Registry, el broker y ZooKeeper con 143 (la salida normal de la JVM ante SIGTERM); ninguno por SIGKILL (`da7a728`).
  - Las paradas del pipeline son limpias: `tests/integration/test_containers.py` exige `run_finished` con totales exactos y salida 0.
  - Antes, con 60 s de gracia para ThingsBoard, `make down` tardaba 61,2 s, todo el periodo de gracia, porque ThingsBoard ignora SIGTERM.
  - Ahora, el `docker compose down` de todos los perfiles al final de las tres sesiones locales completas de cada perfil del 2026-10-12 tardó 5,0-5,8 s (core) y 6,1-21,4 s (full), según el resumen "integration stack" de pytest. El caso de 21,4 s no se investigó.

### 2.3 ThingsBoard ignora SIGTERM
- **Problema.** En `thingsboard/tb-postgres:4.2.1.1`, el proceso PID 1 es `start-tb.sh`, un bash sin `trap` que no reenvía la señal. Comprobado el 2026-10-10: SIGTERM, y SIGKILL 60 s después, salida 137. Postgres hace recuperación de fallo en cada arranque.
- **Acción.** Periodo de gracia de 5 s, documentado. La opción (b), un script propio con `trap`, queda en el backlog sin programar.
- **Resultado.** Sin resolver. Por eso F7 (tiempo de drenaje tras una caída de ThingsBoard) no se midió: requiere una parada controlada de ThingsBoard.
- **F5 y F6 son otra cosa:** las aplazó el owner por decisión de alcance el 2026-10-12 (DETENTE 3.3.g), no por SIGTERM. Quedan fuera del README, con su elemento de backlog (`b538247`).

### 2.4 Pérdidas en la cola de Mosquitto, y su corrección
- **Problema.** Con los valores por defecto de Mosquitto, un bridge que se retrasaba menos de un segundo perdía mensajes: Mosquitto descartaba los nuevos y seguía mandando el PUBACK al publicador.
  - El límite documentado es `max_queued_messages` 1000, sin límite de bytes. El medido en el contenedor (Mosquitto 2.1.2) fue de **1040 mensajes por cliente**: 1040 de 5000 llegaron en el control (`97ecefe`), y la sección 0 de la nota de diseño lo recoge.
  - Se perdieron 56 880 y 72 329 mensajes a 3000 msg/s (bloque 3).
  - Se perdieron 785 a 2000 msg/s (r2 del bloque 4). **Atribuirlos a este mecanismo es una inferencia:** r2 no tiene datos `$SYS` ni log de Mosquitto (se guardan desde `141a259`); solo coincide el patrón.
- **Acción.** Diagnóstico a 3000 msg/s con la ventana completa (`0dcf77b`), que confirmó el mecanismo: 61 140 descartados según `$SYS/broker/publish/messages/dropped`, exactamente los 61 140 que faltaban en `telemetry.raw`. Corrección aprobada en el DETENTE 3.3.h:
  - cola de 60 000 mensajes y 25 MiB, con su regla de dimensionado (`baef69b`);
  - un monitor de descartes en el bridge, en una conexión propia (`5c225e0`);
  - un test de pausa en CI y un test de desbordamiento en `integration-full` (`97ecefe`);
  - un descarte de Mosquitto invalida la corrida (`61c009f`).
- **Resultado.**
  - El test de pausa contra la configuración antigua (control): 1 040 de 5 000 mensajes llegaron, 3 960 descartados. Con la corrección: 5 000 de 5 000, 0 descartados.
  - Desbordamiento: 65 000 mensajes; descartados, faltantes y registrados por el bridge coinciden (4 960). Mosquitto ocupó 47,5 MiB en el límite.
  - En la serie final no hubo ningún descarte en ninguna corrida. A 3000 msg/s el pipeline acumuló un lag de 8,3 y 14,0 s sin perder nada.
  - Semántica de entrega documentada para la Task 3.4 (`b772893`).

### 2.5 La carga del healthcheck JVM
- **Problema.** `cub kafka-ready` y `cub zk-ready` lanzan una JVM en cada comprobación, cada ~11 s, dentro del contenedor del broker (en sus vCPU fijadas) y del de ZooKeeper. **Todas las corridas anteriores a la configuración final llevaron esa carga.**
- **Acción.** Comparaciones en reposo de 300 s, con el mismo método del arnés. Healthchecks sin JVM (`cd711fc`). El reporter de telemetría y Self-Balancing de cp-server, apagados (`7825d3e`).
- **Resultado** (sección 10 de `docs/internal/phase3_task3.3h_mosquitto_queue_design.md`):
  - broker en reposo: media del 18,0 % de un núcleo con `cub kafka-ready` y 5,9 % con `nc -z`; muestras ≥ 50 %: 17 frente a 1, y 15 de esas 17 coinciden con una ejecución del healthcheck;
  - con las funciones de cp-server apagadas: media del 19,6 al 15,8 %, y de 5 a 1 los tópicos internos `_confluent*` creados;
  - cinco arranques en frío seguidos con la configuración nueva: los cinco pasan, sin errores de aprovisionamiento ni de arranque.

### 2.6 El coste del log por mensaje
- **Problema.** El bridge escribe a INFO una línea `message_delivered` por mensaje (el valor por defecto del producto, con el que se mide).
- **Acción.** Bloque de sensibilidad: el bridge en WARNING a 2000 msg/s (comparación) y a 3000 msg/s (diagnóstico, no publicable). Los dos primeros intentos fallaron por un defecto del arnés: esperaba una línea INFO que un bridge en WARNING no escribe. Se corrigió leyendo el contador de suscripciones de Mosquitto (`8d5abfc`).
- **Resultado** (tabla de sensibilidad de la sección 1; CPU del bridge de `cpu.csv` en la ventana medida):
  - a 2000 msg/s, la CPU media del bridge baja del 81-87 % de un núcleo al 62 %, y el p99 de e2e de unos 26 ms a 12 ms; **es una sola corrida en WARNING**;
  - a 3000 msg/s en WARNING, el pipeline aguantó (lag de 0,12 s, p99 de e2e de 52 ms) mientras en INFO se retrasaba;
  - el producto no cambia: INFO sigue por defecto y 2000 msg/s es la cifra publicada; el cambio queda en el backlog (`b538247`).

## 3. Reglas cambiadas después de ver datos

| Fecha | Regla | Motivo | Commit |
|---|---|---|---|
| 2026-10-11 (DETENTE 3.3.f) | El límite de CPU de 0,8 núcleos se aplica solo a los instrumentos (generador y medidor). El bridge y el consumer se juzgan solo por D8. | En el bloque 3, las dos corridas a 2000 msg/s eran inválidas solo por "CPU-bound client(s): bridge at 102 % of a core", con el pipeline dentro de D8. Un bridge a un núcleo es el sistema bajo prueba trabajando, no un fallo de medida. Consta en `docs/internal/benchmark_step1_design.md`. | `df684f6` |
| 2026-10-11 | Cierre de la escalera: un escalón inválido con un instrumento en su límite termina la escalera ("limited by the measuring tools"), sin escalón intermedio. | A 3000 msg/s el generador estaba al 97-98 % de un núcleo. F3 se cerró en 2000 por decisión del owner. | `3d24ec3` |
| 2026-10-11 | E2 cuenta solo las corridas de la última revisión del arnés. Ventana de reloj corregida (desde la muestra anterior al salto). | No mezclar corridas de antes y después de un cambio del arnés. La ventana anterior no recogía ninguna muestra negativa. | `3d24ec3` |
| 2026-10-11 | **F3 reabierta.** Tras la pérdida de 785 mensajes en r2 a 2000 msg/s, el owner contó r2 como repetición fallida y reabrió dónde cierra F3 (el 3.3.f la había cerrado en 2000). | El mecanismo se confirmó ese mismo día con el diagnóstico a 3000 msg/s (`0dcf77b`). F3 quedó resuelta por la serie sobre la configuración final: 2000 msg/s, confirmado 5 de 5. | `86edf0b`, `0dcf77b` |
| 2026-10-11/12 (DETENTE 3.3.h) | Una corrida es válida solo si Mosquitto no descartó ningún mensaje. | El mecanismo de pérdida de la sección 2.4. | `61c009f` |
| 2026-10-12 (DETENTE 3.3.i) | Solo las corridas de la configuración final alimentan las tablas publicadas. Las anteriores quedan como historia, etiquetadas. | El producto cambió (cola, monitor, healthchecks, cp-server). | `d53be71` |
| 2026-10-12 (DETENTE 3.3.g) | Sustitución en E2: un intento inválido solo por un instrumento en su límite (sin pérdidas ni descartes de Mosquitto) se sustituye por su repetición válida. Cualquier otro intento inválido cuenta y hace fallar E2. | r3 a 2000 msg/s: el generador se detuvo 272 ms (al 102 % de un núcleo); nada se perdió y Mosquitto descartó 0. A diferencia del r2 del bloque 4, que perdió 785 mensajes. El informe muestra los seis intentos. | `8d5abfc`, `47c040b` |

## 4. Limitaciones que acompañan a cada cifra

- **Todas.** Un solo portátil: WSL2 con 8 vCPU fijadas por servicio, todos los componentes en el mismo host y red Docker local; no dicen nada de máquinas separadas ni de redes reales. Ventanas de 300 s, no horas. Marcas de tiempo de milisegundo. Los saltos de reloj de WSL2 son conocidos: las muestras negativas se cuentan y nunca se descartan.
- **F1 (e2e) e `ingest`:** se miden desde el instante programado, así que una parada del generador cuenta como latencia del pipeline (el intento r3, sección 3). A 5 msg/s no hay muestras suficientes para el p99.
- **F2 (alarma):** con un patrón de fallo fijo (temperatura del rodamiento alta 10 s de cada 20). A 5 y 50 msg/s, en los 5 activos reales; los 100 clones (20 por activo) solo se usan en las corridas de throughput, así que F2 a 2000 msg/s es con 100 activos (`bench/harness.py`, `execute`).
- **F3:** 2000 msg/s es el escalón más alto medido válidamente. El de 3000 lo limita el generador, aunque el pipeline también se retrasaba ahí en INFO. No se midió ningún escalón entre 2000 y 3000. Bridge en INFO, el valor por defecto.
  - **El generador está cerca de su propio límite a 2000 msg/s en el stack completo:** en las cinco corridas válidas usó de media el 55,0-60,6 % de un núcleo, con picos del 68-76 %, frente a un límite de 80 %.
  - Uno de los seis intentos se saturó: pico del 102 %, media del 57,8 % y una parada de 272 ms (CPU de `cpu.csv` en la ventana medida; picos del manifiesto).
- **F4 (`produce_to_ack`):** Kafka escribe en tmpfs con un solo broker y factor de replicación 1, así que `acks=all` no cuesta nada más. Es optimista frente a un Kafka en disco y replicado.
- **Carga y entrega:** mensajes sintéticos de unos 218 B. Antes de que el bridge reciba un mensaje, la entrega es como mucho una vez, con un búfer acotado y sin contrapresión. Lo publicado con el bridge caído se pierde (backlog).
- **Calibración:** no se repitió en la configuración final (los instrumentos no cambiaron). El tope de la escalera (3000) sale de las corridas de calibración anteriores.
- **ThingsBoard:** F5, F6 y F7 no se publican. Lo verificado y que se puede afirmar consta en el backlog: el límite de 10 msg/s por activo y la entrega sin pérdidas en dos tests de integración.

## 5. Lo que queda sin verificar

- El mecanismo del modo lento del reenvío de Docker Desktop, y por qué el linger de 5 ms cuesta 12-13 ms en Windows (STEP 0).
- Los picos de CPU del broker con carga (156-189 % de un núcleo). Caen en los primeros 13 s del envío y ningún evento registrado los explica (sección 11 del documento de diseño).
- Por qué el generador se detuvo 272 ms en el intento r3.
- Que los 785 mensajes perdidos en r2 (bloque 4, primera serie) se debieran a la cola de Mosquitto: es una inferencia; esa corrida no tiene datos `$SYS` ni log de Mosquitto.
- Cuánta parte de cada mejora entre la serie anterior y la final se debe a cada cambio: la cola, los healthchecks y cp-server cambiaron a la vez.
- El coste del log con más de una corrida en WARNING.
- La memoria de ZooKeeper y su CPU en reposo después de cambiar su healthcheck (solo se probó `srvr` en un contenedor aparte).
- F5, F6 y F7 (ThingsBoard).

## 6. Desvíos respecto a los tres briefings

**STEP 0 (protocolo).** Los diez desvíos están en la sección 6 de `docs/internal/benchmark_protocol_proposal.md`. Los principales:
- no existe `src/`, y `ts_to_ack_ms` ya no existía;
- el origen de las cifras de 16 frente a 42-44 ms no está en el repo;
- cuatro pasadas en vez de una (suspensión, desenchufe, batería), todas conservadas;
- el experimento `bimodal.py` se añadió a la matriz;
- el agente ejecutor (Claude Code) relajó por su cuenta el criterio de corriente alterna en la pasada 3 y el owner lo restauró.

**STEP 1 (implementación).** En `docs/internal/benchmark_step1_design.md` ("Desvíos y pendientes") y en `docs/internal/benchmark_step2_plan.md` ("Desvíos de la implementación"):
- commit de partida `9ace303`, no `78673d2`;
- el bridge leyó sus ajustes de productor de `config/` (D-F), lo que levantó la restricción de no tocar el bridge;
- el generador programa sobre el reloj de pared de la VM;
- la pendiente del lag admite 0,001 s/s;
- las funciones de E/S del arnés llevan `pragma: no cover`.

**STEP 2 (corridas).** El briefing preveía 29 corridas (36 en el peor caso) en cinco bloques y prohibía cambiar código durante el step salvo un arreglo aprobado. Lo que pasó:
- **Cambios aprobados a mitad del step, en este orden:**
  - DETENTE 3.3.f: la regla de CPU, la calidad del reloj y el informe;
  - el cierre de F3 y E2 por revisión;
  - el registro de Mosquitto y el modo diagnóstico;
  - **cambios en el producto** (DETENTE 3.3.h y 3.3.i): la cola de Mosquitto, el monitor de descartes, los healthchecks y cp-server;
  - el ejecutor de series, la sustitución en E2 y la espera por `$SYS`.
- **Corridas no previstas:**
  - dos diagnósticos a 3000 msg/s;
  - **una serie completa repetida** sobre la configuración final;
  - comparaciones en reposo (fuera de `bench/results/`).
- El bloque de sensibilidad se corrió una sola vez, al final y sobre la configuración final, y el diagnóstico a 3000 msg/s se pidió como no publicable.
- Los intentos sin resultados se conservan como directorios incompletos (`20261012T183625Z` y `20261012T184014Z`, con su `compose.override.yml`).
- **El commit `44088cc` se titula "block sensitivity (2 runs)", pero contiene esos dos intentos fallidos sin resultados.** Lo escribió el ejecutor de series antes de la corrección, que contaba un intento sin resultados como corrida. El bloque WARNING real está en `b7cb2f9`, con el mismo título.
- F3 se reabrió el 2026-10-11 y se resolvió con la serie final (sección 3); en el plan del briefing, la escalera (bloque 3) daba el candidato y las repeticiones E2 (bloque 4) lo confirmaban.

## 7. Corridas y horas de portátil

Fuente: los manifiestos de `bench/results/`, reevaluados con las reglas actuales. Las sesiones agrupan corridas separadas por menos de 15 minutos.

| | Válidas | Inválidas |
|---|---|---|
| Configuración final | 19 (10 latencia, 8 throughput, 1 sensibilidad) | 4 (3 throughput por un instrumento, el diagnóstico WARNING a 3000) + 2 intentos sin resultados |
| Corridas anteriores (historia) | 27 (5 calibración del generador, 1 del medidor, 10 latencia, 10 throughput, 1 prueba) | 7 (2 calibración del generador, 3 throughput, 2 diagnósticos) |

- **Horas de portátil enchufado en el STEP 2: 7,95 h** en 8 sesiones (del primer arranque al último final, más el enfriamiento inicial). Dentro de las corridas, 5,42 h. Los dos intentos sin resultados suman unos 8 minutos más.
- Fuera de `bench/results/`: dos comparaciones en reposo de unos 15 minutos cada una, cinco arranques en frío (unos 15 minutos), los tests de integración y el STEP 0.

## 8. Notas para el cierre

- **`pragma: no cover`.** Las funciones de E/S del benchmark lo llevan: en `bench/harness.py`, la orquestación (`Run`, `execute`, las calibraciones, `SysSampler`, `SubscriptionWatch`, `CpuSampler` y los auxiliares del host y de Docker); el `main()` de `bench/generator.py`, `bench/meter.py` (y `ClockSampler.run`), `bench/loader.py` y `bench/report.py`; y el `main()` de `bench/series.py`. Esto va más allá de la regla del repositorio de excluir solo `main()`. Las decisiones (criterios, escalera, E2, series, informe) tienen tests unitarios: `bench/series.py` y `bench/analysis.py` al 100 %. La orquestación se ejercita con las corridas reales.
- **Tolerancia de la pendiente del lag: 0,001 s/s, no 0** (DETENTE 3.3.d, antes de ver datos). Con 0 exacto, el ruido alrededor de un lag estable da pendientes positivas pequeñas y haría fallar corridas estables. 0,001 son 0,3 s ganados en 300 s, por debajo del límite de 1 s de lag. Está en `config/benchmark.yaml` (`criteria.max_lag_slope_per_s`).
