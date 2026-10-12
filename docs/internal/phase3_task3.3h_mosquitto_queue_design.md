# Task 3.3, DETENTE 3.3.h — Cola de Mosquitto para el bridge: nota de diseño

Estado: propuesta para aprobación. Nada implementado, ninguna corrida lanzada.

Origen: el diagnóstico a 3000 msg/s con ventana completa (`bench/results/20261011T233640Z-throughput-diagnostic-3000-r1`, commit `0dcf77b`) confirmó el mecanismo de pérdida. El bridge no recibe a la velocidad de llegada; la cola de Mosquitto para su cliente llega a unos 1040 mensajes; a partir de ahí Mosquitto descarta lo que llega y sigue enviando el PUBACK al publicador. Lo descartado (`$SYS/broker/publish/messages/dropped`, 61 140) coincide exactamente con lo que falta en `telemetry.raw` (61 140). Decisión del owner: corregirlo en el producto, en la misma rama, con commits de producto separados de `bench/`.

Convención: **evidencia** es lo medido o leído en el código; **inferencia** es lo deducido.

## 0. Hechos de partida (evidencia)

| Hecho | Fuente |
|---|---|
| `mosquitto.conf` no fija `max_queued_messages`, `max_queued_bytes`, `max_inflight_messages` ni `sys_interval`: rigen los valores por defecto (1000, sin límite, 20, 10 s) | `broker/mosquitto/mosquitto.conf`; límite observado de unos 1040 en el contenedor 2.1.2 |
| El hilo de red de paho solo encola cada mensaje en `self.inbox`, una `queue.Queue` sin límite, y lo confirma (PUBACK) al volver de `_on_message` | `bridge/mqtt_to_kafka/__main__.py`, `_on_message` |
| A 3000 msg/s la cola de Mosquitto para el bridge se quedó en 870–1096 mientras el backlog del pipeline crecía hasta unos 62 000: el retraso estaba en Mosquitto (más lo descartado), no en el `inbox` | `mosquitto_sys.csv` y `series.csv` del diagnóstico |
| Bytes por mensaje en cola: **217,5 B** de carga útil (`store/messages/bytes` / `store/messages/count` en el límite; 30 actualizaciones, entre 215 y 218 B) | `mosquitto_sys.csv` del diagnóstico |
| A 2000 msg/s, el backlog máximo de las corridas válidas fue de 193–331 mensajes (muestreo cada ~5 s). El r2 inválido alcanzó 2017 y perdió 785 | `series.csv` y `counters.json` de las 8 corridas a 2000 |
| Capacidad media de recepción del bridge en saturación: unos 2 800 msg/s (928 860 entregados en unos 330 s de envío) | diagnóstico (inferencia aritmética) |
| paho-mqtt 2.1.0 (fijado en `requirements.txt`): `clean_session=None` pasa a `True` con MQTT 3.1.1, el protocolo por defecto | `paho/mqtt/client.py:784-785` |

**Inferencia importante para el dimensionamiento.** Una parada de Kafka **no** llena la cola de Mosquitto. El hilo de red sigue recibiendo y confirmando, y el retraso se acumula en el `inbox` del bridge, en memoria y sin límite. A Mosquitto solo llega una parada del **hilo de red del bridge**: proceso congelado, CPU o GIL saturados, o un contenedor en pausa. Una desconexión no cuenta, porque con sesión limpia descarta la sesión entera (punto 6).

## 1. Cola de Mosquitto: finita y mayor, con guarda de memoria

### Regla de dimensionamiento

```
N (max_queued_messages) = R_max × T_parada
B (max_queued_bytes)    = 2 × N × b_msg, redondeado hacia arriba a MiB
Memoria en el límite    ≈ N × (b_msg + sobrecarga por mensaje)
```

| Parámetro | Valor | Origen |
|---|---|---|
| R_max, tasa máxima declarada | **2000 msg/s** | El escalón candidato de F3, la mayor tasa medida como sostenible. Si F3 cierra en otro valor, N se recalcula con la misma regla |
| T_parada, segundos de parada del bridge absorbidos | **30 s** | La pérdida transitoria observada (r2: pico de 2017 en backlog y 785 perdidos, unos 2 800 mensajes, ~1,4 s a 2000 msg/s) cabe con un margen de unas 20 veces. 30 s está por debajo del umbral de keepalive (1,5 × 60 s): una parada más larga desconecta al bridge y con sesión limpia se pierde todo de todos modos |
| b_msg | **218 B** (medido) | Ver sección 0 |
| **N** | **60 000** | 2000 × 30 |
| **B** | **25 MiB (26 214 400 B)** | 2 × 60 000 × 218 = 26,2 MB. Con mensajes del tamaño medido el límite efectivo es N; B solo actúa si la carga media por mensaje en cola supera el doble de la medida (p. ej., payloads anómalos o de otro productor) |
| Memoria de Mosquitto en el límite | **≤ 25 MiB de carga útil + sobrecarga; estimación ≤ 60 MB en total** | La sobrecarga por mensaje (estructuras del store y de la cola, topic) **no está medida**. Supuesto de planificación: ≤ 500 B por mensaje (30 MB). Se mide en el test del punto 3 (memoria del contenedor con `docker stats` en el pico) y se informa |

Cambios en `broker/mosquitto/mosquitto.conf`, con comentarios que expliquen la regla y los números:

```
max_queued_messages 60000
max_queued_bytes 26214400
sys_interval 10        # el valor por defecto, ahora explícito: el bridge depende de él (punto 2)
```

`max_inflight_messages` se queda en 20 (sin cambio).

### Qué pasa más allá del límite

- Mosquitto descarta los mensajes QoS 1 nuevos para ese cliente en cuanto se alcanza N o B, lo primero que ocurra. Sigue enviando el PUBACK al publicador, incrementa `publish/messages/dropped` y escribe una sola vez "Outgoing messages are being dropped for client X." Es el mismo comportamiento de hoy, con un límite 58 veces mayor.
- El límite es **por cliente**. Cada suscriptor lento puede retener hasta N mensajes. Hoy el bridge es el único suscriptor de telemetría en producto; las sondas de los tests y del smoke también lo son mientras corren.
- **Sobrecarga sostenida** (llegada por encima de la capacidad del bridge): ningún límite finito evita la pérdida, solo la retrasa. El tiempo hasta llenar la cola es N / (R_llegada − C_bridge). Inferencia sobre el diagnóstico: a 3000 msg/s frente a unos 2 800 de capacidad, 60 000 / ~185 ≈ 5 minutos. Con la cola llena, la latencia añadida es de N / C_bridge ≈ 21 s. La cola convierte pérdida en latencia; a 3000 msg/s D8 seguiría fallando por p99 en vez de por pérdida.
- No hay contrapresión hacia el publicador. MQTT 3.1.1 no la tiene, y Mosquitto confirma la publicación, no la entrega.

Descartado: cola ilimitada (`max_queued_messages 0`). En sobrecarga sostenida la memoria de Mosquitto crece sin cota hasta que el host o el contenedor lo matan, y entonces se pierde todo lo que hay en cola.

## 2. Visibilidad: el bridge vigila el contador de descartes de Mosquitto

### Dónde vive

- **Módulo nuevo** `bridge/mqtt_to_kafka/drop_monitor.py`, con una clase pura `DropCounter`. Su método `observe(valor) -> delta | None` hace lo siguiente:
  - el primer valor fija la línea base y no avisa;
  - un aumento devuelve el delta;
  - un valor igual devuelve `None`;
  - un valor menor indica que Mosquitto se reinició: fija una nueva base y devuelve un evento de reinicio;
  - un payload no numérico devuelve un error.
- **Conexión MQTT propia**: un segundo cliente paho dentro del proceso del bridge, con client id `<client-id>-drops`, suscrito con QoS 0 a `$SYS/broker/publish/messages/dropped`. Va por una conexión aparte y no por la del bridge porque esa conexión es justo la congestionada. Por ella, la actualización de `$SYS` quedaría detrás de hasta N mensajes de telemetría, o sería descartada con ellos. **No verificado:** si Mosquitto 2.1.2 descarta, y cuenta, también los QoS 0 de un cliente con la cola llena. La conexión separada hace innecesario averiguarlo.
- **Eventos de log** (JSON, como el resto del bridge):
  - `mqtt_messages_dropped`, **ERROR**, campos `delta`, `total`, `broker`, `scope: "broker-wide"`, en cada aumento;
  - `mosquitto_drop_counter_reset`, WARNING;
  - `mosquitto_drop_counter_silent`, WARNING, si no llega ningún valor en 2 × `sys_interval`, por ejemplo con `$SYS` desactivado.
  - El resumen final (`run_finished`) añade `mosquitto_dropped_during_run`.
- **Arranque**: si la conexión de vigilancia no se suscribe en `CONNECT_TIMEOUT_S`, el bridge no arranca. Es la misma regla de fallo rápido que con Kafka, Schema Registry o la suscripción de telemetría: la visibilidad pasa a formar parte de la garantía.

### Límites de la señal (documentarlos)

- El contador es **de todo el broker**, no del bridge. Cuenta descartes de cualquier cliente, por ejemplo una sonda de test lenta. El evento lo dice (`scope`).
- Retraso de detección: hasta `sys_interval`, 10 s.
- Es un **aviso, no una recuperación**: lo descartado no se puede pedir otra vez.
- Si Mosquitto publica ese `$SYS` como retenido, la base llega al suscribirse. Si no, llega en la primera actualización, y un descarte en los primeros ≤ 10 s tras el arranque del bridge queda dentro de la base. **No verificado en 2.1.2**; lo dirá el test de integración.

### Tests

- **Unitarios** (`tests/unit/test_bridge.py` o un archivo nuevo junto a él): `DropCounter` en todos sus casos (base, aumento, sin cambio, reinicio, payload inválido) y el cableado. Un mensaje `$SYS` simulado llega al callback de la conexión de vigilancia y produce el evento ERROR con su delta (con `caplog`, como los tests actuales del bridge). La cobertura de rama del bridge se mantiene en su nivel actual.
- **Integración**: ver el punto 3, caso "desbordamiento visible".

### Coste

Una conexión TCP más, un mensaje cada 10 s y un hilo de red de paho más en el proceso: CPU despreciable frente al ~1 núcleo del bridge con carga (inferencia, no medido). Código: unas 60 líneas más los tests.

## 3. Tests

### Test de integración nuevo: el bridge en pausa no pierde nada (`tests/integration/test_mqtt_queue.py`)

Usa el bridge en contenedor, con los fixtures de `test_containers.py`, y lo pausa con `docker compose pause bridge`. El freezer de cgroups congela el proceso con la conexión TCP abierta. Funciona igual en Windows y en CI, cosa que no permite un `SIGSTOP` a un proceso del host.

1. Marca los offsets de `telemetry.raw` y lee la base de `publish/messages/dropped`.
2. Pausa el bridge.
3. Publica **K = 5 000** mensajes válidos con QoS 1 y pares (asset, seq) únicos. 5 000 es casi 5 veces el límite antiguo y queda muy por debajo del nuevo.
4. **Comprobación de que el test ejercita el límite antiguo:** durante la pausa, `$SYS/broker/store/messages/count` debe superar 1 100. Si no lo supera, los mensajes se quedaron en buffers TCP y el test no prueba nada. Entonces el test falla, no pasa en vacío.
5. La pausa dura como mucho **30 s**, por debajo del keepalive.
6. Reanuda el bridge y espera a que estén los K en `telemetry.raw`.
7. Exige:
   - los K pares (asset, seq) en Kafka, sin faltantes;
   - **delta de descartes = 0**, leído tras una actualización completa de `$SYS`;
   - ningún evento `mqtt_messages_dropped` en el log del bridge.
8. Registra la memoria de Mosquitto en el pico (`docker stats`) para el punto 1.

**Control negativo**, una vez y como evidencia: el mismo test contra el `mosquitto.conf` actual debe fallar (unos 4 000 descartes esperados). Se ejecuta antes del cambio de configuración y se informa.

### Segundo caso: el desbordamiento es visible

Con el mismo montaje, publica N + 5 000 = 65 000 mensajes durante la pausa (≤ 30 s a unos 3 000 msg/s desde el test). Exige:

- la suma de los `delta` de `mqtt_messages_dropped` = el delta de `$SYS` = los que faltan en Kafka;
- y que esos faltantes sean > 0.

Así se prueban a la vez el límite nuevo y la señal del punto 2. Duración estimada: ~1–2 minutos. Si resulta demasiado lento para CI, se marca como solo para `test-integration-full` y se informa.

### Smoke

`scripts/smoke/run_smoke.py` y `run_smoke_containers.py` leen `publish/messages/dropped` antes de empezar y después de que el bridge termine, esperando una actualización de `$SYS`, unos 10 s más. Añaden una fila "Mosquitto dropped: 0" que forma parte del veredicto. También comprueban que no aparece `mqtt_messages_dropped` en el log del bridge.

### Validez de cada corrida del benchmark (commit de `bench/`, aparte)

- El arnés ya guarda `manifest["mosquitto"]["dropped_total"]` y la serie.
- Se añade a `bench/criteria.py` un motivo de invalidez propio: `Mosquitto dropped N message(s)`. Toma el delta de `publish/messages/dropped` entre el inicio y el final de la corrida.
- Hoy una pérdida ya invalida por `max_lost: 0`. El motivo nuevo la atribuye y además detecta un descarte de otro cliente. Ningún umbral de `config/benchmark.yaml` cambia; es un criterio nuevo y fijo a 0.
- Tests unitarios en `tests/unit/test_bench_harness.py`.

## 4. Documentación: la garantía de entrega exacta (texto en inglés para `docs/ARCHITECTURE.md`)

**Corrección a la formulación pedida.** "At-least-once from bridge receipt onward" no es exacto con el código actual:

- el bridge confirma (PUBACK) al recibir y guarda lo recibido en memoria (el `inbox` y la cola de librdkafka);
- una caída del proceso pierde esos mensajes; solo una parada limpia los procesa y los vacía (`shutdown`);
- además, el productor idempotente no duplica.

Lo exacto es **"no loss and no duplicate from receipt onward while the bridge runs; a crash loses what it holds"**. Propongo este texto:

> **Delivery semantics, MQTT to Kafka**
>
> - **Publisher → Mosquitto.** QoS 1. A PUBACK means Mosquitto accepted the message. It does not mean the bridge received it, or ever will.
> - **Mosquitto → bridge.** QoS 1, clean session, no broker persistence. While the bridge is connected, Mosquitto holds up to 60,000 messages (or 25 MiB of payload) for it. Beyond either bound it drops new messages for the bridge and still acknowledges the publisher: there is no backpressure. Drops are visible: the bridge logs `mqtt_messages_dropped` at ERROR with the count. The counter is broker-wide and updated every 10 s. Messages published while the bridge is not connected are not delivered (no persistent session).
> - **Inside the bridge.** Each MQTT message is acknowledged on receipt, before Kafka confirms it, and held in memory until produced. A clean stop (SIGTERM, SIGINT) processes and flushes everything received; a crash loses it.
> - **Bridge → Kafka.** `acks=all` with the idempotent producer: retries neither duplicate nor reorder a record. A record not confirmed within librdkafka's `delivery.timeout.ms` (default 300 s) is logged as `message_delivery_failed`, and the run exits 1.
> - **In short.** From receipt by a running bridge onward, every message reaches `telemetry.raw` once, unless the bridge crashes or a delivery failure is logged. Before receipt, delivery is at most once, through a bounded buffer with no backpressure; losses at the buffer are logged, losses while the bridge is down are not.

**`docs/ARCHITECTURE.md` no existe todavía**; es un entregable de la Task 3.4. Propuesta: crearlo ahora solo con esta sección, y que la 3.4 lo complete. El backlog ya pide documentar ahí la semántica de cada salto. La alternativa es guardar el texto en `phase3_backlog.md` hasta la 3.4.

El docstring del bridge ("Delivery semantics") se actualiza en el mismo commit que el monitor, para que diga lo mismo.

## 5. Funciones de cp-server no usadas (solo propuesta)

**Evidencia** (`raw/broker.log` del diagnóstico, configuración volcada al arrancar):

| Ajuste efectivo | Efecto observado |
|---|---|
| `confluent.balancer.enable = true` | Crea `_confluent_balancer_partition_samples` (32 particiones, 23:36:58) y `_confluent_balancer_api_state` (~23:38:04) |
| `confluent.reporters.telemetry.auto.enable = true` → `metric.reporters = [io.confluent.telemetry.reporter.TelemetryReporter]` | Exportador `_local` hacia Kafka: crea `_confluent-telemetry-metrics` (23:37:57), grupo de consumo `ConfluentTelemetryReporterSampler` (rebalanceo a las 23:41:06), recolección cada 60 s |
| `confluent.telemetry.enabled = false` | Ya no se envía nada a Confluent. Es lo que dice el comentario de `docker-compose.yml`, pero eso no apaga el exportador local |

Nada del repositorio lee esos tópicos (`git grep _confluent`: ninguna referencia fuera de los resultados). Self-Balancing no tiene nada que hacer con un solo broker.

**Ajustes exactos** (`docker-compose.yml`, servicio `broker`; la imagen traduce `KAFKA_X_Y` a `x.y`):

```yaml
KAFKA_CONFLUENT_BALANCER_ENABLE: "false"
KAFKA_CONFLUENT_REPORTERS_TELEMETRY_AUTO_ENABLE: "false"
```

Con eso se eliminan la parte de balancer de `KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR` (deja de tener efecto, se puede quitar) y el comentario que hoy da a entender que la telemetría está apagada del todo.

**Qué cambia:**

- dejan de crearse los tópicos internos citados y el grupo de consumo del muestreador;
- menos hilos y menos trabajo periódico en el broker;
- la licencia y su tópico `_confluent-license` siguen, porque cp-server los exige.

**No cambia:** ninguna función que use el pipeline. La alternativa de cambiar la imagen a `cp-kafka` 6.1.1 (Apache Kafka sin las funciones de Confluent Server) es un cambio mayor y no la propongo ahora.

**Cómo mostrar el efecto en la CPU del broker:**

1. A/B en reposo. El stack arriba sin carga, 5 minutos con la configuración actual y 5 con la nueva, con el muestreador de CPU del arnés. Se comparan la mediana, el p95 y las muestras ≥ 1,2 núcleos, y se comprueba que en `broker.log` no aparecen las creaciones de esos tópicos. Unos 15 minutos con corriente.
2. Con carga. Si se adopta, E2 a 2000 se mide después (punto 7). Se comparan la CPU del broker (mediana, p95, muestras ≥ 1,2 núcleos), `produce_to_ack` y la latencia e2e con el E2 del bloque 4, sabiendo que el cambio de cola va en la misma tanda. El A/B en reposo es lo que aísla el efecto de cp-server.

**Recomendación:** decidirlo aparte de la cola. Si se adopta, conviene que entre antes de volver a medir, para no repetir E2 dos veces.

## 6. Backlog (no se hace ahora): lo publicado con el bridge caído o reiniciándose se pierde

**Verificado en el código:**

- paho 2.1.0, `clean_session=None` → `True` con MQTT 3.1.1 (`client.py:784-785`). El bridge no pasa `clean_session` (`start_mqtt`).
- Mosquitto corre con `persistence false`.
- Client id `mqtt-to-kafka-bridge-<pid>`. **Matiz nuevo:** en contenedor, con `init: true`, el PID se repite (el diagnóstico muestra `-7`), así que el id **no** cambia entre reinicios del contenedor; en el host sí cambia. Da igual mientras la sesión sea limpia: al conectar, Mosquitto descarta cualquier sesión anterior.
- Sin suscriptor conectado, Mosquitto acepta y confirma cada QoS 1 y lo descarta. Tampoco lo cuenta como `dropped`, porque no hay cola de cliente que desborde, así que el monitor del punto 2 **no** lo ve.
- Lo mismo pasa en una reconexión automática de paho tras un corte de red: lo publicado entre la desconexión y la nueva suscripción se pierde (`_on_connect` vuelve a suscribir, con sesión limpia).

**El backlog ya lo recoge** (`phase3_backlog.md`, sección Task 3.4, entrada del 2026-10-03, "A simulator QoS 1 acknowledgement means…"). Propuesta: actualizar esa entrada en el commit de documentación. Se añaden el matiz del PID en contenedor, la reconexión, que esas pérdidas no aparecen en `$SYS` dropped, y que la sesión persistente (client id fijo, `clean_session=False`, `persistence true`) quedaría acotada por los mismos N y B de este punto 1. No se crea una entrada duplicada.

## 7. Plan de nueva medición tras la corrección

**Etiquetado.** `HARNESS_REVISION` pasa a "after the 3.3.h product fix" (commit de `bench/`). Las corridas anteriores conservan la suya y no se tocan. El informe muestra la revisión de cada corrida, y E2 cuenta solo las de la última. El `max()` lexicográfico actual de `report.py` funciona por casualidad ("h" > "f"); en el mismo commit lo cambio por un orden explícito de revisiones, con test.

| Bloque | ¿Se repite? | Por qué |
|---|---|---|
| E2 a 2000 msg/s, 5 corridas en INFO | **Sí** | Es la cifra publicable y el producto cambió. Las 6 anteriores quedan etiquetadas como de antes de la corrección; r2 sigue contando como repetición fallida de esa revisión |
| Sensibilidad en WARNING, a 2000 y a 3000, 1 corrida cada uno | **Sí** (primera vez, era el bloque 5) | Con el producto nuevo |
| Escalón 3000 en INFO | **No** | El generador estaba al 98 %: sigue limitado por el instrumento, la corrección no lo cambia |
| Escalera 250, 500 y 1000 | **No** | El backlog máximo fue mucho menor de 1040: la cola nueva no interviene. Se quedan con la etiqueta de antes de la corrección |
| Latencia (5 y 50 msg/s, 10 corridas) | **Sin cambio en cp-server: 1 corrida de control a 50 msg/s.** Con cp-server cambiado: el bloque entero | A 5 y 50 msg/s la cola de Mosquitto está en su base y el monitor añade 0,1 msg/s. Si la corrida de control cae dentro del rango del bloque 2, el bloque 2 se mantiene. El cambio de cp-server altera el broker y con él `produce_to_ack` |
| Calibración (bloque 1) | **No** | El generador solo, sin suscriptor, y el medidor leyendo Kafka: ninguno pasa por la cola de un suscriptor |

**Tiempo con corriente**, estimado con la cadencia medida del bloque 4, ~9 min por corrida con 120 s de enfriamiento:

| Tarea | Sin cambio en cp-server | Con cambio en cp-server |
|---|---|---|
| Control negativo y tests de integración nuevos, smoke | ~15 min | ~15 min |
| A/B en reposo de cp-server | — | ~15 min |
| E2 a 2000 (5) | ~45 min | ~45 min |
| Sensibilidad (2) | ~18 min | ~18 min |
| Latencia | ~9 min (1) | ~90 min (10) |
| **Total** | **~1 h 30 min** | **~3 h** |

## 8. Commits previstos (tras la aprobación)

1. `fix(mosquitto)`: `max_queued_messages`, `max_queued_bytes` y `sys_interval` explícito, con comentarios. Antes, el control negativo del test.
2. `feat(bridge)`: `drop_monitor.py`, conexión de vigilancia, eventos de log, docstring, tests unitarios.
3. `test(integration)`: `test_mqtt_queue.py` (pausa sin pérdidas y desbordamiento visible); fila de descartes en ambos smokes.
4. `docs`: sección de semántica de entrega (punto 4) y entrada del backlog actualizada (punto 6).
5. `chore(broker)`, solo si se aprueba el punto 5: los dos ajustes de cp-server.
6. `feat(bench)`, aparte: motivo de invalidez por descartes, `HARNESS_REVISION`, orden explícito de revisiones, tests.

Ningún cambio en los umbrales de `config/benchmark.yaml`. Ninguna corrida hasta que el plan del punto 7 esté aprobado.

## 9. Decisiones del owner

1. Valores de la cola: R_max = 2000 msg/s, T = 30 s → N = 60 000, B = 25 MiB. ¿Se aceptan, o se dimensiona con otra T o R?
2. Monitor de descartes con conexión propia y fallo rápido al arrancar: ¿se acepta?
3. Garantía de entrega: ¿se acepta el texto corregido del punto 4 en lugar de "at-least-once from bridge receipt onward"? ¿Se crea `docs/ARCHITECTURE.md` ahora, solo con esa sección?
4. cp-server: ¿se adoptan los dos ajustes, y antes de volver a medir?
5. Latencia: ¿basta con 1 corrida de control a 50 msg/s si cp-server no cambia?
6. El caso de desbordamiento visible (65 000 mensajes): ¿en CI, o solo en `test-integration-full`?

## 10. Medido después de la aprobación (DETENTE 3.3.i)

Esta sección se añadió tras implementar y medir; las secciones 1 a 9 son la propuesta tal como se aprobó. Todas las mediciones en reposo usan el mismo método: stack core recreado y aprovisionado, 300 s sin carga, `docker stats` del arnés (una muestra cada ~2 s), 120 s de enfriamiento entre variantes, con corriente alterna y sin contenedores ajenos. Una sola pareja por comparación, siempre en el mismo orden: confianza baja en diferencias pequeñas.

| Cambio (commit) | Medida | Antes | Después |
|---|---|---|---|
| Cola de Mosquitto 60 000 / 25 MiB (`baef69b`) | Test de pausa, 5 000 mensajes con el bridge congelado | 1 040 en Kafka, 3 960 descartados (control) | 5 000 de 5 000, 0 descartados |
| Ídem | Desbordamiento, 65 000 mensajes con el bridge congelado | — | 60 040 en Kafka, 4 960 descartados = faltantes = registrados por el bridge; Mosquitto con 47,5 MiB en el límite |
| Funciones de cp-server desactivadas (`7825d3e`), con el healthcheck JVM en ambas variantes | CPU media del broker en reposo (% de un núcleo) | 19,6 | 15,8 |
| Ídem | Muestras ≥ 1,2 núcleos / ≥ 50 % | 8 / 20 | 5 / 16 |
| Ídem | Tópicos internos `_confluent*` creados; rebalanceos | 5; 2 | 1 (`_confluent-license`); 1 |
| Healthcheck del broker sin JVM (`cd711fc`), con las funciones de cp-server activas en ambas variantes | CPU media del broker en reposo | 18,0 | 5,9 |
| Ídem | Muestras ≥ 1,2 núcleos / ≥ 50 % | 4 / 17 (15 de las 17 coinciden con una ejecución del healthcheck) | 1 / 1 (sin healthcheck cerca) |
| Ídem | Duración de cada comprobación (mediana) | 1 329 ms (`cub kafka-ready`, una JVM) | 67 ms (`nc -z`) |
| Healthcheck de ZooKeeper sin JVM (`cd711fc`) | ZooKeeper, muestras ≥ 50 % de un núcleo, sin cambiar su healthcheck | 11 en cada variante, 21 de 22 coinciden con una ejecución de `cub zk-ready` (0,75 s cada una) | No medido en reposo tras el cambio; `srvr` tarda 12 ms frente a 0,67 s de `zk-ready` (contenedor aparte) |

**Toda corrida hecha hasta ahora llevó la carga del healthcheck JVM.** El override de compose del benchmark fija las CPU de cada servicio pero no toca los healthchecks (comprobado en `compose.override.yml` de las corridas). En cada corrida de los bloques 1 a 4 y en los diagnósticos, Docker lanzó cada ~11 s una JVM de `cub kafka-ready` dentro del contenedor del broker, en sus dos vCPU fijadas (0-1), y una de `cub zk-ready` en la vCPU 7, compartida por ZooKeeper y Schema Registry. Inferencia, no medida con carga: parte de los picos de CPU del broker vistos en el bloque 3 y en el diagnóstico, y quizá parte de la cola de `produce_to_ack`, vienen de ahí. La serie de nueva medición sobre la configuración final es la que lo dirá.

## 11. Decisiones del DETENTE 3.3.g (serie sobre la configuración final)

**E2 a 2000 msg/s: el intento r3 inválido se sustituye por su repetición válida; 2000 msg/s queda confirmado (5 de 5).** Decisión del owner. Motivo: a diferencia del r2 de la serie anterior (785 mensajes perdidos), en este intento no se perdió nada (Mosquitto descartó 0) y la causa es el instrumento: el generador se detuvo 272 ms (lateness máxima 272 ms, 102 % de un núcleo), y la latencia medida desde el instante programado cuenta esa parada como latencia del pipeline (e2e p99 1 380 ms en ese intento). El bridge estaba al 105 % de un núcleo, como en las corridas válidas (97-104 %). La regla, aplicada por `bench/report.py`: un intento inválido solo por un instrumento en su límite (sin pérdida, sin descarte de Mosquitto, sin problema de entorno) se sustituye por la siguiente corrida válida con el mismo número de repetición; cualquier otro intento inválido cuenta y hace fallar E2. El informe lista los seis intentos y, para el sustituido, la CPU del bridge y los descartes de Mosquitto.

**Picos de CPU del broker con carga (156-189 % de un núcleo): ningún evento registrado los explica.** Revisado solo leyendo los logs guardados de dos corridas de la escalera (250 y 2000 msg/s). Todas las muestras ≥ 1,2 núcleos caen en los primeros 13 s del envío, dentro del calentamiento que no se mide. El log del broker solo tiene 4 líneas en la ventana, comprobaciones periódicas del controlador (rebalanceo automático de líderes), ninguna cerca de un pico. Las pausas de GC jóvenes ocurren cada ~2,5 s (131 en 330 s, máx. 35 ms) y caen cerca del 92 % de todas las muestras, así que coincidir con un pico no dice nada. Inferencia, no comprobada: el arranque de la carga (conexiones, calentamiento de la JVM). Va en las limitaciones.
