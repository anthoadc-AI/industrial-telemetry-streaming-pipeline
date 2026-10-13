# Task 3.4: tabla de trazabilidad de `docs/ARCHITECTURE.md`

Cada afirmación sobre el comportamiento del sistema que hace el documento, con su evidencia en el repo. Las líneas son las de la rama `docs/phase3-architecture` en el commit que añade este fichero. Algunas afirmaciones se repiten en varias secciones del documento; aparecen aquí una vez, en la primera.

Tipos de evidencia:
- **cfg**: fichero de configuración o compose.
- **cód**: código o su comentario de módulo.
- **test**: test automático.
- **vivo**: consulta a un sistema en marcha, con el comando y la salida.
- **int**: documento interno.

Lo que solo sale de leer el código, sin test que lo ejercite, se marca "sin test". El documento lo dice igual.

Los valores de las tablas del documento (topics, umbrales, ajustes, versiones) los comprueba además `tests/unit/test_architecture_doc.py`, en el job `checks`.

## 1. Alcance y datos sintéticos

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 1.1 | MQTT sin autenticación | `broker/mosquitto/mosquitto.conf:7` (`allow_anonymous true`) | cfg |
| 1.2 | Listeners de Kafka en PLAINTEXT | `docker-compose.yml:101-102` | cfg |
| 1.3 | Un broker, RF 1, una instancia de cada componente | `config/topics.yaml` (`replication_factor: 1`); servicios únicos en `docker-compose.yml` | cfg |
| 1.4 | Cinco activos, cuatro canales, modelo físico | `config/telemetry.yaml` (`assets`, `channels`); `edge/simulator/model.py` | cfg, cód |
| 1.5 | Dos modos de fallo: `bearing_overheating` (rampa, temperatura) e `imbalance` (escalón, vibración) | `config/telemetry.yaml:95-113` | cfg |
| 1.6 | Los umbrales son ilustrativos y elegidos para los datos sintéticos | `config/alarms.yaml:3-6` | cfg |
| 1.7 | `synthetic` en cada registro de telemetría y de alarma; `injected_fault` | `schemas/telemetry_v1.avsc`, `schemas/alarm_v1.avsc`; `edge/simulator/__main__.py:239-241`; `state_machine.js:114` (`synthetic` en el evento) | cód |

## 2-4. Componentes y flujo

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 2.1 | Un mensaje por activo y ciclo, QoS 1, en `plant/<asset_id>/telemetry` | `config/topics.yaml:9-14`; `edge/simulator/__main__.py:76-78`, `138-139` | cfg, cód |
| 2.2 | Ritmo por defecto: un ciclo por `sample_interval_s` (1.0 s) | `edge/simulator/__main__.py:76-78`; `config/telemetry.yaml:9` | cód, cfg |
| 2.3 | El bridge se suscribe a `plant/+/telemetry` con sesión limpia | `config/topics.yaml:13`; `bridge/mqtt_to_kafka/__main__.py:331`; sesión limpia: `docs/internal/phase3_backlog.md:14` (paho 2.1.0 `client.py:784-785`, verificado el 2026-10-12) | cfg, cód, int |
| 2.4 | Las seis reglas de validación; sin comprobación de rango, y por qué | `bridge/mqtt_to_kafka/validation.py:1-23` | cód |
| 2.5 | Avro vía Schema Registry a `telemetry.raw`, clave `asset_id` | `bridge/mqtt_to_kafka/__main__.py:1-12` | cód |
| 2.6 | Lo inválido va a `telemetry.dlq` intacto, con clave del activo del topic MQTT; códigos de razón | `bridge/mqtt_to_kafka/__main__.py:9-12`, `360`, `387-428`; `validation.py` (`invalid_json`, `invalid_ts`, `schema_mismatch`, `non_finite_value`, `topic_mismatch`) | cód |
| 2.7 | Las cartas muertas del consumer: indecodible y `missing_field` | `processing/alarm_consumer/consumer.js:30-44`, `217-245`; `test_dead_letters_real` | cód, test |
| 2.8 | El sink: entrada de telemetría con el `ts` de la muestra; evento de alarma bajo `alarm_<channel>_<severity>`; `raised_ts` sacado del `alarm_id` | `processing/thingsboard_sink/__main__.py:14-23`, `135-157`; `test_sink_forwards_telemetry_with_the_sample_timestamps` | cód, test |
| 2.9 | Un dispositivo por activo, creado con su tipo, y por tanto con su perfil | `processing/thingsboard_sink/__main__.py:9-12`, `237-245`; `test_sink_forwards_telemetry_with_the_sample_timestamps` | cód, test |
| 2.10 | La Root Rule Chain guarda y pasa a AlarmMapping. AlarmMapping no evalúa umbrales; regla de aplicación; `alarm_last` | `scripts/thingsboard/import_rule_chains.py:8-14`; `processing/thingsboard/rule-chains/alarm-mapping.json:57` | cód |
| 2.11 | `TELEMETRY_TOPIC` solo sustituye el nombre de `telemetry.raw` | `config/topics.yaml:3-4`; `tests/unit/test_config_loader.py` | cfg, test |
| 2.12 | Los dos cargadores coinciden | `tests/unit/test_alarm_config_equivalence.py:106` | test |

## 5. Topics y esquemas

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 5.1 | Topics creados solo por `provision.py`; autocreación apagada | `broker/kafka/provision.py:1-16`; `docker-compose.yml:110` | cód, cfg |
| 5.2 | Tabla de topics (particiones, RF, ajustes, esquema) | `config/topics.yaml:16-44`; `test_architecture_doc.py::test_topics_table_matches_config` | cfg, test |
| 5.3 | La DLQ del sink va con clave `alarm_id` | `processing/thingsboard_sink/__main__.py:379` (`key=msg.key()` de `telemetry.alarms`) | cód |
| 5.4 | Valores por defecto del broker en vigor | ver "Consulta en vivo" al final | vivo |
| 5.5 | `telemetry.raw` y `telemetry.alarms` sin ajustes de topic aparte de `cleanup.policy` | ver "Consulta en vivo" (overrides dinámicos) | vivo |
| 5.6 | Clave `alarm_id`, partición por `asset_id` con murmur2; por qué; los dead letters siguen la misma regla | `processing/alarm_consumer/consumer.js:328-382` (`assetOfKey`, `AssetPartitioner`, `createProducer`); `test_alarms_of_an_asset_share_one_partition` | cód, test |
| 5.7 | La compactación no acota por tiempo (segmento activo) | `docs/internal/phase3_backlog.md:11` (verificado el 2026-10-02); `tests/integration/test_restore_compaction.py:1-30` | int, test |
| 5.8 | El test ajusta los valores del cleaner solo para él y rueda las particiones | `tests/integration/test_restore_compaction.py:37-38`, `83` | test |
| 5.9 | Sujeto `<topic>-value`; compatibilidad global BACKWARD, fijada y releída | `schemas/README.md:3-5`; `broker/kafka/provision.py:12-16`, `37`, `99` | cód |
| 5.10 | Política de versiones | `schemas/README.md:13-28` | int |
| 5.11 | El `doc` de `source` en `dlq_v1` omite el sink | `schemas/dlq_v1.avsc:10`; `processing/thingsboard_sink/__main__.py:376` (`"source": SOURCE`) | cód |

## 6. Lógica de alarmas

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 6.1 | HI y HIHI independientes, cada una con su estado y su `alarm_id` | `processing/alarm_consumer/state_machine.js:3-12`, `44-62`, `120-130` | cód |
| 6.2 | Tablas de umbrales | `config/alarms.yaml:17-46`; `test_architecture_doc.py::test_threshold_tables_match_alarms_config` | cfg, test |
| 6.3 | `speed_rpm` sin alarma | `config/alarms.yaml:15` | cfg |
| 6.4 | Transiciones NORMAL/ACTIVE con persistencia e histéresis | `state_machine.js:13-21`, `64-92`; `tests/unit/state_machine.test.js` | cód, test |
| 6.5 | Rupturas de racha: hueco o retroceso de `seq`, valor nulo o no finito; contadores a cero, estado sin cambio | `state_machine.js:27-31`, `152-170`; `state_machine.test.js` | cód, test |
| 6.6 | Sin debounce temporal ni limpieza periódica | `state_machine.js:22-25` | cód |
| 6.7 | `alarm_id` determinista | `state_machine.js:33-35`, `40-42` | cód |
| 6.8 | Reconstrucción: lee hasta los offsets finales, 60 s como máximo, último evento por clave, tombstone borra, RAISED → ACTIVE | `consumer.js:88`, `153-200`; `state_machine.js:95-99`, `134-146`; `processing/alarm_consumer/index.js:163-173`; `test_restore_after_real_compaction` | cód, test |
| 6.9 | `alarms_restored`, `restored_alarm_without_rule`, `state_restore_failed` | `index.js:168-171` | cód |
| 6.10 | No reconstruye contadores ni último `seq`; un levantamiento o borrado puede retrasarse hasta `persistence_samples − 1` muestras | `state_machine.js:95-99` (`resetCounters`), `152-156` (`lastSeq` indefinido al arrancar). **Sin test** | cód |
| 6.11 | Defecto: CLEARED espurio tras restaurar y reprocesar; qué queda en `telemetry.alarms` y en ThingsBoard | `tests/integration/test_restore_replay.py` (los dos tests); `docs/internal/phase3_task3.4_h3_result.md` | test |
| 6.12 | Commit de offsets al volver los handlers del lote | `consumer.js:9-14`, `262` | cód |
| 6.13 | Una alarma ya borrada que se reprocesa no tiene efecto visible | AlarmMapping, `alarm-mapping.json:57` (descarta lo que no sea más nuevo, salvo el CLEARED de la misma alarma). **Sin test específico** | cód |
| 6.14 | Tipo de alarma `<label> high (warning|critical)` | `processing/thingsboard_sink/__main__.py:144-146` | cód |
| 6.15 | Rama Failure → `alarm_mapping_failed`, sin retener al activo | `alarm-mapping.json:198`; `test_a_mapping_failure_is_recorded_and_does_not_hold_back_the_asset` | cód, test |
| 6.16 | Reconciliación en `make up` | `Makefile:84-87`; `scripts/thingsboard/import_rule_chains.py:522-555`; `test_make_up_reconciliation_clears_an_alarm_lost_with_kafka` | cód, test |

## 7. Garantías de entrega

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 7.1 | Texto MQTT→Kafka aprobado (DETENTE 3.3.h) con las dos precisiones de H1 (decisión A3) | `docs/internal/phase3_backlog.md:15-23` | int |
| 7.2 | Cola por cliente de 60000 mensajes / 25 MiB; sin backpressure; `mqtt_messages_dropped` | `broker/mosquitto/mosquitto.conf:41-42`; `bridge/mqtt_to_kafka/drop_monitor.py:6`, `82`; `test_a_paused_bridge_loses_nothing`, `test_an_overflow_is_logged_with_its_count` | cfg, cód, test |
| 7.3 | Contador de todo el broker, cada 10 s | `mosquitto.conf:47`; `drop_monitor.py:40` | cfg, cód |
| 7.4 | ACK MQTT al recibir; retenido en memoria hasta producirse | `bridge/mqtt_to_kafka/__main__.py:44-46`, `345` | cód |
| 7.5 | (H1b) Parada limpia: procesa lo recibido y hace flush hasta `--flush-timeout` (10 s); lo pendiente va en `run_finished_incomplete`, salida 1 | `__main__.py:37-42`, `145`, `460-489`; `test_sigterm_flushes_simulator_and_bridge` (Linux) | cód, test |
| 7.6 | `acks=all` e idempotencia | `config/bridge.yaml` (`producer`) | cfg |
| 7.7 | (H1a) `message_delivery_failed`; el bridge sigue y sale con 1 al pararse | `__main__.py:367-371`, `481-489` | cód |
| 7.8 | Id de cliente con el pid; se repite en contenedor | `__main__.py:144`; `docker-compose.yml:258` (`init: true`); `phase3_backlog.md:14` | cód, int |
| 7.9 | Reconexión automática y nueva suscripción en cada conexión | `__main__.py:330-331` | cód |
| 7.10 | Esas pérdidas no aparecen en `$SYS/.../dropped` | `docs/internal/phase3_backlog.md:14` | int |
| 7.11 | Orden en el consumer: decodificar, estado, `send` esperado, retorno del handler | `consumer.js:7-14`, `300-317` | cód |
| 7.12 | Fallo de envío: el consumer se para sin commit y sale con 1; reintentos del consumer desactivados | `consumer.js:14-19`, `254-258`; `index.js:22-24`; `test_offset_not_committed_when_handler_throws` | cód, test |
| 7.13 | Schema Registry transitorio: nunca a la DLQ | `consumer.js:35-39`, `202-216`; `test_schema_registry_down` | cód, test |
| 7.14 | Duplicados con el mismo `alarm_id` y la misma clave; por qué no transacciones | `consumer.js:21-28` | cód |
| 7.15 | El sink guarda el offset tras el PUBACK; commit automático de lo guardado | `processing/thingsboard_sink/__main__.py:38-45`, `291-293`, `225-235`, `420` | cód |
| 7.16 | El PUBACK no significa que el motor de reglas lo procesara | `thingsboard_sink/__main__.py:40-43` | cód |
| 7.17 | Colas de ThingsBoard en memoria; un reinicio pierde lo aceptado | `docker-compose.yml:180`; `phase3_backlog.md:32` | cfg, int |
| 7.18 | PipelineSequential: uno a uno por dispositivo, 60 s por lote, 20 reintentos de 5 s a 60 s | `scripts/thingsboard/import_rule_chains.py:106-117` | cód |
| 7.19 | Lo que la cola abandona solo se ve en sus estadísticas (log) | `phase3_backlog.md:34`; `tests/integration/integration_support.py:465` (`queue_stats_since`) | int, test |
| 7.20 | Duplicados en ThingsBoard inofensivos | `thingsboard_sink/__main__.py:43-45`; `test_alarm_ordering_and_replay_leave_exactly_one_alarm_per_alarm_id`; `test_an_event_applied_again_after_a_partial_run_is_absorbed` | cód, test |
| 7.21 | Fallos transitorios del sink: backoff de 0.5 s doblando hasta 30 s; mientras, no lee nada más | `thingsboard_sink/__main__.py:47-51`, `95`, `404-419` | cód |

## 8. Modos de fallo

| # | Fila del documento | Evidencia | Tipo |
|---|---|---|---|
| 8.1 | Bridge caído o desconectado | `phase3_backlog.md:14` (observado el 2026-10-03); `__main__.py:342` (`mqtt_disconnected`) | int, cód |
| 8.2 | Bridge más lento que su entrada | `test_a_paused_bridge_loses_nothing`, `test_an_overflow_is_logged_with_its_count` | test |
| 8.3 | Payload inválido | `test_dead_letters_real` | test |
| 8.4 | Fallo al enviar a la DLQ (bridge) | `__main__.py:18-25`; `test_bridge_dead_letter_send_failure_real` | cód, test |
| 8.5 | Kafka inalcanzable al arrancar: los tres se niegan a arrancar | bridge `__main__.py:14-16`; consumer `index.js:8`, `143-161`; sink `thingsboard_sink/__main__.py:300`, `305` y `65-66`. **Sin test** | cód |
| 8.6 | Kafka no disponible en marcha (bridge): `delivery.timeout.ms`; la cola interna crece sin límite (H2) | `__main__.py:188`, `304-321`, `345`. **Sin test** | cód |
| 8.7 | Fallo al enviar una alarma | `test_offset_not_committed_when_handler_throws` | test |
| 8.8 | Schema Registry caído (consumer) | `test_schema_registry_down` | test |
| 8.9 | Schema Registry caído (sink) | `thingsboard_sink/__main__.py:47-51`, `159-174`, `415`. **Sin test** | cód |
| 8.10 | Schema Registry caído tras el arranque (bridge): sin verificar | H11; ningún test | — |
| 8.11 | Consumer parado: se acumula, reconstruye y reanuda | `test_restore_after_real_compaction` | test |
| 8.12 | Consumer parado entre el envío y el commit | `test_replay_before_the_raise_emits_a_spurious_clear` | test |
| 8.13 | ThingsBoard o el sink caídos: backlog, drenaje al límite por activo, alarmas nuevas detrás | `test_a_backlog_of_one_asset_reaches_thingsboard_without_a_drop`; `thingsboard_sink/__main__.py:28-35` | test, cód |
| 8.14 | ThingsBoard reiniciado | `docker-compose.yml:180`. **Sin test** | cfg |
| 8.15 | AlarmMapping no puede aplicar | `test_a_mapping_failure_is_recorded_and_does_not_hold_back_the_asset` | test |
| 8.16 | Reinicio del broker fuera de `make up` | `phase3_backlog.md:29`; `test_make_up_reconciliation_clears_an_alarm_lost_with_kafka` | int, test |
| 8.17 | Reinicio de Mosquitto | `mosquitto.conf:13`; `drop_monitor.py:13-15`, `77`. **Sin test** | cfg, cód |
| 8.18 | `$SYS` calla 25 s | `drop_monitor.py:21`, `41`, `86-93`; `tests/unit/test_drop_monitor.py` | cód, test |
| 8.19 | Muere el hilo de informes de entrega | `__main__.py:27-35`, `296`; `tests/unit/test_bridge.py` | cód, test |

## 9. Configuración y despliegue

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 9.1 | Tabla de ficheros de configuración | cabeceras de cada fichero de `config/`; `.env.example` | cfg |
| 9.2 | Tabla de ajustes | `test_architecture_doc.py::test_settings_table_matches_its_sources` | test |
| 9.3 | Acoplamiento entre los retardos SQL y el límite del sink; el test que falla si el par deja de ser seguro | `docker-compose.yml:187-201`; `config/thingsboard.yaml:2-19`; `test_a_backlog_of_one_asset_reaches_thingsboard_without_a_drop` | cfg, test |
| 9.4 | Perfiles y objetivos de make | `docker-compose.yml:11-22`; `Makefile:84-117` | cfg |
| 9.5 | `make up-pipeline` corre sin `--duration` (H9: no existe `make run`) | `Makefile:99-101`; `docker-compose.yml` (servicios `bridge`, `alarm-consumer`, `thingsboard-sink`) | cfg |
| 9.6 | `make down` sin `-v` | `Makefile:107-117`; `docker-compose.yml:8-9` | cfg |
| 9.7 | No correr a la vez en el host y en contenedores | `Makefile:35` | cfg |
| 9.8 | tmpfs para Kafka y ZooKeeper, y por qué | `docker-compose.yml:68-72` | cfg |
| 9.9 | `stop_grace_period` en todos los servicios y por qué; 30 s en el pipeline | `docker-compose.yml:35-39`, `259`, `283`, `316`, `337` | cfg |
| 9.10 | ThingsBoard ignora SIGTERM | `docker-compose.yml:168-174`; `phase3_backlog.md:71-73` (verificado el 2026-10-10) | cfg, int |
| 9.11 | Endurecimiento de los contenedores del pipeline; ningún puerto publicado; sin política de reinicio | `docker-compose.yml:258-262`, `284-285`, `317-318`, `338-339`; `docker-compose.yml:32` | cfg |
| 9.12 | Tabla de versiones | `test_architecture_doc.py::test_pinned_versions_appear_in_their_sources` | test |
| 9.13 | Imágenes base por digest; lockfile de Node; acciones de CI por SHA | `docker/pipeline-python/Dockerfile:12`; `docker/alarm-consumer/Dockerfile:10`; `package-lock.json`; `.github/workflows/ci.yml:57-62` | cfg |

## 10. Observabilidad

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 10.1 | Formato JSON común; lo que pasa por él | `common/jsonlog.py` (docstring); `common/jsonlog.js`; `tests/unit/test_jsonlog_equivalence.py` | cód, test |
| 10.2 | Eventos por componente | bridge: `__main__.py:249`, `337`, `342`, `370`, `425`, `296`, `486-488`; `drop_monitor.py:82`, `93`. Consumer: `index.js:128-130`, `168-171`; `consumer.js:271`, `295`, `312`. Sink: `thingsboard_sink/__main__.py:219`, `223`, `334`, `370`, `383`, `415`, `430`, `466`. Importación: `import_rule_chains.py:280`, `540`, `552`, `555` | cód |
| 10.3 | Campos de latencia y sus relojes; error de truncado de 0 a 1 ms | `bridge/mqtt_to_kafka/__main__.py:56-80`; `consumer.js:46-66` | cód |
| 10.4 | Monitor de descartes: línea base, aumento, reinicio, 25 s, se niega a arrancar | `drop_monitor.py:1-41`; `config/bridge.yaml:18-28`; `__main__.py:262`; `tests/unit/test_drop_monitor.py` | cód, cfg, test |
| 10.5 | Avisos de arranque | `phase3_backlog.md:24-27`; `docs/internal/phase2_backlog.md:36`; `consumer.js:371-376` | int, cód |

## 11. Verificación

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 11.1 | Disparadores y cuatro jobs; `integration-full` solo en push a main y en dispatch | `.github/workflows/ci.yml:1-45`, `49`, `73`, `115-118`, `161` | cfg |
| 11.2 | Cobertura de líneas y ramas ≥ 80 % | `pyproject.toml` (`fail_under`, `check_branch_coverage`); `jest.config.js` | cfg |
| 11.3 | El test de sincronía corre en `checks` dentro de `make test` | `pyproject.toml` (`testpaths = ["tests/unit"]`); `Makefile:131-134` | cfg |
| 11.4 | Tabla de tests | nombres comprobados con `grep "^def test_" tests/integration/test_*.py` | test |
| 11.5 | Comprobaciones al cerrar la sesión `full` | `tests/integration/conftest.py:1-19`, `86-133` | test |

## 12. Decisiones de diseño

| # | Decisión | Evidencia | Tipo |
|---|---|---|---|
| 12.1 | Confluent 6.1.1, ZooKeeper, cp-server: heredado, no reevaluado; KRaft y versión actual no evaluados (decisión A6 de Anthony) | `docs/internal/phase0_diagnostic_report.md:142-152`; `docs/internal/claude_code_briefings/phase1_briefing_solid_base.md:62` | int |
| 12.2 | Node.js para el consumer: decisión del dueño en la Fase 2, dos runtimes | `docs/internal/claude_code_briefings/phase2_briefing_engineering_quality.md:29-35`; `docs/internal/phase2_backlog.md:14` | int |
| 12.3 | kafkajs: `node-rdkafka` no funciona con Node 24; riesgo aceptado; alternativa | `docs/internal/phase0_diagnostic_report.md:473`; `docs/internal/phase2_backlog.md:35`; `docs/internal/phase2_closure_report.md:360` | int |
| 12.4 | ThingsBoard 4.2.1.1: la versión que corría el curso, fijada tal cual en la Fase 0 | `docs/internal/phase0_diagnostic_report.md:35`, `320`, `388` | int |
| 12.5 | Un solo motor de alarmas | `docs/internal/claude_code_briefings/phase3_briefing_quantified_evidence.md:76`; `import_rule_chains.py:12-18` | int, cód |
| 12.6 | Al menos una vez sin transacciones | `consumer.js:21-28` | cód |
| 12.7 | Clave `alarm_id` y partición por activo | `consumer.js:338-358`; `test_alarms_of_an_asset_share_one_partition` | cód, test |
| 12.8 | Avro y BACKWARD | `schemas/README.md`; `provision.py:12-16` | cód |
| 12.9 | Asimetría de la DLQ | `bridge/mqtt_to_kafka/__main__.py:18-25`; `consumer.js:40-44` | cód |
| 12.10 | Sin comprobación de rango | `validation.py:18-20` | cód |
| 12.11 | tmpfs; la opción B queda abierta (H10) | `docker-compose.yml:68-72`; `docs/internal/benchmark_protocol_proposal.md:282`, `400` | cfg, int |
| 12.12 | Límite del sink acoplado | `config/thingsboard.yaml:2-19` | cfg |
| 12.13 | PipelineSequential frente a SequentialByOriginator | `import_rule_chains.py:28-43`, `106-117`; `phase3_backlog.md:34` | cód, int |

## 13-14. Limitaciones y Firebase

| # | Afirmación | Evidencia | Tipo |
|---|---|---|---|
| 13.1 | Las limitaciones 1-19 | cada una remite a su sección del documento (filas de arriba) o al backlog: `phase3_backlog.md`, sección "From Task 3.4" y las de la Task 3.4 anteriores | int |
| 13.2 | Dashboard: línea recta en un hueco, sin contador de descartes, mes natural, sin texto de error | `phase3_backlog.md:28`, `37` | int |
| 13.3 | Dependencias transitivas de Python sin bloquear | `requirements.txt`; `phase3_backlog.md:60` | cfg, int |
| 14.1 | Nodo REST "Firebase" en la Root Rule Chain; desactivado por defecto | `processing/thingsboard/rule-chains/root-rule-chain.json:138-149`; `.env.example:46-49` | cfg |
| 14.2 | Poda cuando `FIREBASE_ENABLED` no es `true`; `firebase_leg_pruned`; sustitución de la URL | `import_rule_chains.py:19-24`, `187-195`, `231`, `275-282`, `596-597` | cód |
| 14.3 | `PUT ${FIREBASE_DB_URL}/temperature.json` para cada mensaje de telemetría | `root-rule-chain.json:148-149` | cfg |
| 14.4 | Nada lee `FIREBASE_DB_SECRET` | búsqueda en el repo: solo `.env.example:49` y `scripts/check_secrets.py:30` (lista de permitidos) | cód |
| 14.5 | Nunca verificado contra una base real | `import_rule_chains.py:21-23` | cód |

## Consulta en vivo (decisión A5)

Fecha: 2026-10-12, a las 01:13 UTC del 13 según el log de aprovisionamiento. Stack levantado con `make up-core` sobre `3871605` y aprovisionado. Después se paró con `make down`. Versión del broker según `kafka-topics --version`: `6.1.1-ce (Commit:2b0dc7d5743ffa8d)`.

Comando, para cada topic (se filtran las claves citadas y se quitan los sinónimos):

```
docker compose exec -T broker kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name <topic> --describe --all
```

| Clave | `telemetry.raw` | `telemetry.alarms` | `telemetry.dlq` |
|---|---|---|---|
| `cleanup.policy` | delete | compact | delete |
| `retention.ms` | 604800000 | 604800000 | 604800000 |
| `retention.bytes` | -1 | -1 | -1 |
| `segment.ms` | 604800000 | 604800000 | 604800000 |
| `segment.bytes` | 1073741824 | 1073741824 | 1073741824 |
| `min.cleanable.dirty.ratio` | 0.5 | 0.5 | 0.5 |
| `delete.retention.ms` | 86400000 | 86400000 | 86400000 |
| `min.compaction.lag.ms` | 0 | 0 | 0 |
| `max.compaction.lag.ms` | 9223372036854775807 | 9223372036854775807 | 9223372036854775807 |
| `min.insync.replicas` | 1 | 1 | 1 |

Overrides dinámicos (`--describe` sin `--all`):
- `telemetry.alarms`: solo `cleanup.policy=compact`.
- `telemetry.raw`: ninguno.

El `retention.ms` de `telemetry.dlq` sí lo fija `config/topics.yaml`. Coincide con el valor por defecto.

Broker (`--entity-type brokers --entity-name 1 --describe --all`):
- `log.cleaner.backoff.ms=15000`
- `log.cleaner.enable=true`
- `log.retention.hours=168`
- `log.roll.hours=168`
- `log.segment.bytes=1073741824`

## Lo que el documento no afirma, a propósito

- Ninguna cifra medida: ni 6.9, 20.2 ni 43.1 msg/s, ni el drenaje de 7 a 60 minutos (decisión A4). Solo el valor configurado de 10 msg/s por activo.
- No dice con qué frecuencia aparece el defecto H3: no se ha medido.
- No hace ninguna afirmación positiva sobre el bridge con Schema Registry caído tras el arranque (H11).
