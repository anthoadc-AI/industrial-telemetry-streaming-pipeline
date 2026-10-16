# Informe de cierre de la Fase 3 — Evidencia cuantificada

| Campo | Valor |
|---|---|
| Proyecto | industrial-telemetry-streaming-pipeline |
| Fase | 3 de 3, Evidencia cuantificada |
| Briefings | `docs/internal/claude_code_briefings/phase3_briefing_quantified_evidence.md` (fase) y los briefings de tarea de la misma carpeta (3.3 STEP 0 a 2, 3.4, 3.5, el arreglo de H3, 3.6) |
| Fechas de trabajo | 2026-10-02 a 2026-10-15 |
| Máquina | Windows 11 Home 10.0.26200, Docker Desktop (Engine 29.0.1, WSL 2, 8 vCPU y 12 GB para la VM), Python 3.13.4, Node.js 24.16.0, GNU Make 3.81 (GnuWin32) |
| CI | GitHub Actions, `ubuntu-24.04`, cuatro jobs |
| Estado | **Listo para revisión (DETENTE 3.6.b).** Rama `chore/phase3-publication`, sin merge, sin tag. El repositorio sigue privado. |

La versión en inglés de este informe es `docs/internal/phase3_closure_report.en.md`; las dos dicen lo mismo, con las mismas cifras, hashes y rutas. Todos los datos producidos en esta fase son sintéticos.

---

## 1. Resumen

La Fase 3 convirtió un pipeline probado pero invisible en evidencia que un revisor puede comprobar. Al cierre:

- **Capa de operación.** Un sink de ThingsBoard reenvía la telemetría y los eventos de alarma a ThingsBoard por su API MQTT de gateway; `make up` crea un dashboard de operación, las cadenas de reglas y los dispositivos sin ningún paso manual. ThingsBoard no evalúa ningún umbral: el consumidor de alarmas en Node.js es el único motor de alarmas.
- **Benchmark.** Un arnés en `bench/` con su configuración en `config/benchmark.yaml`; cada corrida se guarda en `bench/results/` y `make bench-report` regenera cada cifra publicada. Throughput sostenido 2000 msg/s, confirmado 5 de 5; latencias en la sección 4.
- **Documentación.** `docs/ARCHITECTURE.md` (14 secciones, tres diagramas Mermaid, mantenido igual a `config/` y al compose por un test unitario), `README.md` y `docs/CASE_STUDY.md`, cuyas cifras, enlaces y citas de línea comprueban tests unitarios.
- **Defectos encontrados y corregidos.** El benchmark y las revisiones encontraron defectos del producto que los tests no habían encontrado: Mosquitto descartaba mensajes ya confirmados, un healthcheck cargaba el broker que comprobaba, Docker Desktop mataba contenedores al cabo de 1 s, y dos defectos de repetición de alarmas. Cada uno se corrigió con un test que falla con el código anterior (sección 3).
- **Preparación para publicar.** Un escaneo de todo el historial no encontró ningún secreto; se quitaron del árbol las rutas locales y los nombres de otros proyectos; `make install` y `make test` pasan en un clon nuevo.

La fase no midió el camino hacia ThingsBoard (F5, F6, F7) ni fijó el árbol de dependencias de Python; los dos están en el backlog (sección 6).

---

## 2. Qué se entregó, por tarea

| Tarea | Entregado | Merge en `main` |
|---|---|---|
| 3.0 | Verificación de estado de la fase, solo lectura (DETENTE 3.0.a), con una errata al párrafo 3.1-B del briefing | — |
| 3.1-A | El tiempo de confirmación del bridge se toma en un hilo dedicado a los informes de entrega, no cuando el bucle principal hace poll; `ts_to_ack_ms` eliminado y sustituido por cuatro campos por etapa (`event_to_receive_ms`, `receive_to_produce_ms`, `produce_to_ack_ms`, `event_to_ack_ms`) y `receive_ts_ms`; el consumidor de alarmas registra `consume_to_alarm_ack_ms` | `9b6df92` |
| 3.1-B | `prune_firebase()`: con Firebase desactivado, la importación quita los tres nodos REST, las dos cadenas que quedan vacías y los nodos de entrada que llegaban a ellas, idempotente en los dos sentidos | `cc575b5` |
| (3.1) | Un test de compactación determinista y aislamiento de procesos por test en la suite de integración | `f28516f` |
| 3.2 | Sink de ThingsBoard (Python): API MQTT de gateway, un dispositivo por activo, eventos de alarma mapeados por una cadena de reglas (AlarmMapping) con clave `alarm_id`, dashboard como código, reconciliación de alarmas en `make up`; después, la cola PipelineSequential, retardos de lote SQL de 25 ms y un límite del sink de 10 msg/s por activo, tras dos ejecuciones rojas en `main` | `47f0a78`, `7c690e8`, `832ab50` |
| 3.3 | El pipeline en contenedores (`make up-pipeline`, `make smoke-containers`, un job de CI propio); el arnés del benchmark, sus corridas y `make bench-report`; tres arreglos del producto encontrados por el benchmark | `53a53a0` |
| 3.4 | `docs/ARCHITECTURE.md` y `tests/unit/test_architecture_doc.py`; la revisión encontró el defecto H3 | `a69859b` |
| Arreglo H3 | Una guarda de repetición por el timestamp del último evento de cada alarma; corrige H3 y la alarma fantasma encontrada con él | `c576f2b` |
| 3.5 | `README.md`, `docs/CASE_STUDY.md` y `tests/unit/test_public_docs.py` | `f21c941` |
| (3.5) | Una carrera entre una alarma y su `alarm_last` en los tests del perfil completo, tras una ejecución roja en `main` | `0604a08` |
| 3.6 | Auditoría del STEP 0; rutas locales y nombres quitados; una regla `drive-path` en `scripts/check_secrets.py`; citas de línea comprobadas por contenido; la frase sobre el modo de trabajo en el README y en el caso de estudio; este informe | rama `chore/phase3-publication` |

Historial de git: 140 commits en `main` desde `v2-engineering-quality` (2026-10-02), 14 de ellos merges `--no-ff`, ninguno hecho directamente en `main`; 8 más en la rama de la Task 3.6, este informe incluido. El hook pre-commit ejecuta el escaneo de secretos en cada commit.

---

## 3. Hallazgos (problema, acción, resultado)

**El tiempo de confirmación incluía el retardo del poll (3.1-A).**
- *Problema.* `ts_to_ack_ms` se tomaba en el callback de entrega, que el bridge atendía solo cuando su bucle principal hacía poll del productor, tras esperar hasta 100 ms en su bandeja de entrada. Al ritmo del smoke, la confirmación menos el produce tenía una mediana de 106 ms.
- *Acción.* Se midió y descartó `Message.latency()`: en confluent-kafka 2.15.1 dio 60.4, 100.5 y 300.4 ms con `poll()` retrasado 0, 100 y 300 ms. Se eligió un hilo dedicado bloqueado en `poll()` (`fe37d8f`).
- *Resultado.* Con el hilo principal ocupado 0, 100 y 300 ms, p50 63.7, 56.2 y 57.5 ms. Un test unitario falla con el diseño de la Fase 2 (110.8 ms frente a un broker simulado de 20 ms).

**Cadenas de reglas con una URL de Firebase sin resolver (3.1-B).**
- *Problema.* Con `FIREBASE_ENABLED=false`, la importación dejaba tres nodos REST cuya URL era el marcador `${FIREBASE_DB_URL}`; cada mensaje habría ejecutado una llamada REST fallida.
- *Acción.* `prune_firebase()` (`609cf0b`).
- *Resultado.* Sobre las exportaciones versionadas: 3 nodos REST y 2 nodos de entrada quitados, `TempToFirebase` y `AlarmToFirebase` no se importan; los tests del perfil completo comprueban el estado podado.

**Un test de compactación que dependía del azar.**
- *Problema.* `test_restore_after_real_compaction` falló en la ejecución de CI 37070295558: el broker evalúa `segment.ms` solo cuando escribe en una partición, y el cleaner nunca compacta el segmento activo.
- *Acción.* `roll_every_partition()` escribe un tombstone con una clave reservada en cada partición; cada proceso que arranca un test se para al terminar el test (`5b28fd1`).
- *Resultado.* La compactación sigue siendo la del propio broker y el test es determinista; el comportamiento está documentado en `docs/ARCHITECTURE.md`, sección 5.

**ThingsBoard descartaba y reordenaba eventos de alarma (3.2).**
- *Problema.* Con el particionador por defecto, las alarmas de un activo se repartían entre particiones: 19 de 48 alarmas conservadas tras una repetición. La cola SequentialByOriginator de ThingsBoard (2 s por lote, 3 reintentos) descartaba eventos de alarma (ejecución 37227357007: 43 de 48 alarmas, 108 mensajes descartados). Los dispositivos creados por un cliente distinto del sink iban a la cola paralela Main (41 de 200 alarmas aplicadas fuera de orden, ejecución 37215619234).
- *Acción.* Eventos de alarma particionados por activo (`AssetPartitioner`); la cola PipelineSequential, retardos de lote SQL de 25 ms y un límite del sink de 10 msg/s por activo; la importación crea el dispositivo de cada activo con su tipo.
- *Resultado.* Un backlog de 3,200 mensajes de un activo llega sin descartes (320 s); una ráfaga de 400 eventos de alarma directa al gateway, todos aplicados; la sesión completa falla si ThingsBoard descartó algún mensaje.

**Mosquitto descartaba mensajes que ya había confirmado (3.3).**
- *Problema.* La cola por cliente de Mosquitto admitía 1040 mensajes, medido; un bridge que se retrasaba menos de un segundo perdía mensajes mientras el publicador recibía su PUBACK. Diagnóstico a 3000 msg/s: 61,140 descartados, exactamente los que faltaban en `telemetry.raw`.
- *Acción.* Una cola de 60,000 mensajes y 25 MiB (`baef69b`), un monitor de descartes en el bridge (`5c225e0`), dos tests de integración (`97ecefe`), y una corrida con un descarte de Mosquitto es inválida (`61c009f`).
- *Resultado.* Un bridge en pausa recibió 1,040 de 5,000 mensajes antes y 5,000 de 5,000 después; ninguna corrida de la serie final tuvo un descarte.

**Un healthcheck cargaba el broker que comprobaba (3.3).**
- *Problema.* `cub kafka-ready` y `cub zk-ready` arrancaban una JVM cada ~11 s en las vCPU fijadas del broker.
- *Acción.* Healthchecks sin JVM (`cd711fc`); el telemetry reporter de cp-server y Self-Balancing desactivados (`7825d3e`).
- *Resultado.* Broker en reposo de 18.0 % a 5.9 % de un núcleo de media; muestras en o por encima del 50 %, de 17 a 1.

**Docker Desktop paraba los contenedores al cabo de 1 s (3.3).**
- *Problema.* El proxy de la API de Docker Desktop creaba cada contenedor con un `StopTimeout` de 1 s; el consumidor de alarmas necesita de 1.5 a 3.6 s para pararse.
- *Acción.* Un `stop_grace_period` explícito para cada servicio (`7386596`, `da7a728`).
- *Resultado.* Cada servicio sale con 0 o 143, ninguno por SIGKILL, salvo ThingsBoard, cuyo script de arranque ignora SIGTERM (backlog).

**Un reinicio podía borrar una alarma todavía activa, o levantar una fantasma (3.4, arreglo H3).**
- *Problema.* Tras una parada entre el envío de una alarma y el commit de su offset, las muestras repetidas borraban una alarma restaurada como activa (H3), o volvían a levantar con un id nuevo una alarma ya borrada.
- *Acción.* Una guarda de repetición: cada alarma ignora las muestras con timestamp igual o anterior a su último evento; la restauración devuelve el último evento por tipo de alarma, CLEARED incluido (`4b87a8d`).
- *Resultado.* `test_replay_before_the_raise_does_not_clear_the_restored_alarm`, `test_replay_of_a_clear_does_not_raise_a_phantom_alarm` y `test_after_the_replay_thingsboard_keeps_the_alarm_active_until_the_real_clear` fallan con el código anterior al arreglo y pasan después.

**Un test del perfil completo leía `alarm_last` demasiado pronto (tras la 3.5).**
- *Problema.* Ejecución de CI 38486097882 sobre `f21c941`: AlarmMapping crea la alarma antes de guardar `alarm_last`, y un test lo leía una sola vez; su alarma hizo fallar después el test siguiente.
- *Acción.* `alarm_last_when` sondea con un tiempo límite; un test que falla borra las alarmas que levantó (`06ec32c`).
- *Resultado.* La ejecución 38494698202 en la rama y la 38496923949 en `main` (`0604a08`) pasaron los cuatro jobs.

**Rutas locales en archivos versionados (3.6).**
- *Problema.* La auditoría encontró la ruta absoluta del árbol de trabajo en `bench/results/series-20261012T153319Z.log` (líneas 73 y 77) y la de la carpeta del curso en `docs/internal/phase0_diagnostic_report.md` (línea 7). `check_secrets.py` solo detectaba rutas dentro de un perfil de usuario.
- *Acción.* Las dos sustituidas en su propia línea (`4fe3d42`); una regla `drive-path` (`f1ae378`).
- *Resultado.* La regla detecta las dos versiones anteriores y ningún archivo versionado: 957 archivos escaneados, 0 coincidencias, 17 permitidas. Las versiones anteriores siguen en el historial, que no se reescribe.

---

## 4. Cifras finales y su fuente

Condiciones del benchmark para todas las cifras de esta tabla: un portátil, Docker Desktop sobre WSL 2 con 8 vCPU, servicios fijados a vCPU concretas, un broker de Kafka con factor de replicación 1 sobre tmpfs, mensajes sintéticos de unos 218 B, ventanas de 300 s, cinco corridas por fila; mediana y, entre paréntesis, el rango entre corridas, en milisegundos enteros.

| Cifra | Valor | Fuente |
|---|---|---|
| F1, de la muestra al registro leído de Kafka, 50 msg/s | p50 7 (7-8), p95 11 (11-12), p99 13 (12-13) ms; 15000 muestras por corrida | `make bench-report`; `bench/results/` |
| F2, de la muestra al evento de alarma confirmado por Kafka, 50 msg/s | p50 35 (34-36), p95 55 (52-56) ms; 300 muestras por corrida, sin p99 | ídem |
| F1 a 2000 msg/s | p50 6 (6-6), p95 11 (10-16), p99 26 (19-59) ms; 600000 muestras por corrida | ídem |
| F3, throughput sostenido | 2000 msg/s, confirmado en 5 de 5 corridas; 3000 msg/s no medible de forma válida (el generador en su límite de CPU) | ídem |
| Tests unitarios, `main` en `0604a08` | 459 Python, 119 Node.js, todos pasados | Ejecución de CI 38496923949 |
| Cobertura de Python | "Total coverage: 98.54%"; "845/874 branches, 96.68 %" | ídem |
| Cobertura de Node.js | líneas 100 % (1159/1159), ramas 98.7 % (305/309) | ídem |
| Tests de integración | stack core 16 pasados, 16 omitidos; stack completo 32 pasados | ídem |
| Clon nuevo de `main` en Windows (2026-10-15) | `make install` 84 s, rc 0; `make test` 42 s, rc 0: 458 pasados, 1 omitido (enlaces simbólicos), 119 Node.js | Task 3.6 STEP 0, cachés de pip y npm desactivadas |
| Arreglo de Mosquitto | 1,040 de 5,000 mensajes antes, 5,000 de 5,000 después | `docs/internal/phase3_task3.3_closure_report.md`, 2.4 |
| Arreglo del healthcheck | broker en reposo de 18.0 % a 5.9 % de un núcleo | ídem, 2.5 |
| Camino a ThingsBoard | 10 msg/s por activo por configuración; 3,200 mensajes de un activo en 320 s, sin descartes | `config/thingsboard.yaml`; `test_a_backlog_of_one_asset_reaches_thingsboard_without_a_drop` |
| Escaneo de secretos del historial | 1,266 versiones de archivo y todos los mensajes de commit y de tag de todas las refs: 0 secretos; todas las coincidencias de tipo secreto en `docs/internal/` y cubiertas por la lista de permitidos | Task 3.6 STEP 0 |
| Tamaño del repositorio | pack 2.41 MiB; 957 archivos versionados, 633 en `bench/results/` | `git count-objects -vH`, Task 3.6 STEP 0 |

Las cifras de CI de la rama de la Task 3.6 están en la sección 7.

---

## 5. Desvíos respecto a los briefings

Los desvíos dentro de la Task 3.3 están en la sección 6 de `docs/internal/phase3_task3.3_closure_report.md` y no se repiten aquí.

| # | El briefing dice | Hecho | Decisión |
|---|---|---|---|
| 1 | 3.1-A: evaluar `Message.latency()` como opción (c) | Medido y descartado; un hilo dedicado a los informes de entrega | Owner, DETENTE 3.1.a (`fe37d8f`) |
| 2 | 3.3: `benchmarks/` y `make benchmark` | `bench/` con `make bench-trial`, `make bench-plan` y `make bench-report` | Briefing del STEP 1 de la Task 3.3 (`bench/results/`) |
| 3 | 3.3 STEP 2: ningún cambio en el pipeline, el arnés ni `config/` durante las corridas, salvo un arreglo aprobado | Cuatro cambios del producto en mitad del STEP 2 (la cola de Mosquitto, el monitor de descartes, los healthchecks, cp-server), y después una serie repetida completa | Owner, DETENTE 3.3.h y 3.3.i |
| 4 | 3.3: cifras del camino hacia ThingsBoard | F5 y F6 aplazadas, F7 no medible mientras ThingsBoard ignore SIGTERM; fuera del README | Owner, DETENTE 3.3.d y 3.3.g |
| 5 | 3.3: la escalera de throughput termina en el primer escalón que incumple los criterios | Termina en 3000 msg/s porque el generador llegó a su límite de CPU, no por los criterios del pipeline; ningún escalón entre 2000 y 3000 | Owner, regla cambiada después de ver datos (informe de la Task 3.3, sección 3) |
| 6 | 3.6: una segunda herramienta de escaneo de secretos, como gitleaks | Los patrones de `check_secrets.py` más otros (tokens de servicios comunes, URLs con credenciales, correos, rutas, nombres de máquina) sobre cada blob y mensaje, con un script de un solo uso no versionado | Briefing de la Task 3.6, STEP 0 |
| 7 | 3.6: `pip-audit` y `npm audit` | No ejecutados | Briefing de la Task 3.6 (no los incluye); se registra aquí |
| 8 | 3.6: protección de rama en `main` que exija CI | No propuesta | Briefing de la Task 3.6 (no la incluye); se registra aquí |
| 9 | 3.6: máquina limpia con `make up`, `make test-integration`, `make smoke` y el quickstart del README | `make install` y `make test` en un clon nuevo; el stack, los tests de integración y el smoke en los runners limpios de CI; el quickstart se siguió en la Task 3.5 sobre un `.venv` existente | Briefing de la Task 3.6, paso 5 |
| 10 | 3.6: informes de cierre bilingües de cada fase, en `docs/reports/`, con un test que compare cada par | No hecho: las Fases 0 a 2 están solo en inglés, en `docs/internal/`; solo la Task 3.3 y este informe están en los dos idiomas; `docs/reports/` no existe; no hay test de pares | Owner, DETENTE 3.6.a: se registra como desvío, `docs/reports/` no se crea |
| 11 | 3.6 (backlog): fijar el árbol de dependencias de Python | No hecho | Backlog (sección 6) |
| 12 | 3.6 STEP 0: el primer intento de clon nuevo | Falló por la longitud de ruta de Windows en la carpeta temporal de la sesión, no en el repositorio; repetido en una carpeta corta fuera del árbol de trabajo | — |

---

## 6. Lo que deliberadamente no se hizo, y el backlog

Abierto al cierre. La fuente de cada punto es `docs/internal/phase3_backlog.md` salvo que se indique otra; la lista para el usuario es la sección 13 de `docs/ARCHITECTURE.md`.

- **Cifras de ThingsBoard F5, F6 y F7**, y **un wrapper de SIGTERM para ThingsBoard**, que F7 necesita.
- **La línea de log por mensaje del bridge en DEBUG, con un resumen periódico en INFO.** Efecto medido y registrado; habría que volver a medir cada cifra publicada.
- **Fijar el árbol de dependencias de Python** (punto de la Task 3.6, no hecho).
- **Acotar las esperas fijas de 65 s del perfil completo.**
- **H2:** la cola en memoria del bridge no tiene límite.
- **H4:** una sola instancia del consumidor de alarmas, sin que nada lo imponga.
- **H5:** el nodo de Firebase escribe cada mensaje en una misma ruta sin autenticación; nunca verificado contra una base de datos real.
- **H11:** el bridge con Schema Registry caído después del arranque, sin verificar.
- **La documentación de `source` en `dlq_v1`** omite el sink de ThingsBoard; se cambia con el próximo cambio del esquema.
- **La guarda de repetición confía en los relojes de los productores**, y **el retraso tras un reinicio de hasta `persistence_samples − 1` muestras**: documentados, sin cambio decidido.
- **Almacenamiento persistente de Kafka** (un volumen en vez de tmpfs) y **reconciliación solo en `make up`**: opción abierta, sin decidir.
- **Antes de que el bridge reciba un mensaje, la entrega es como mucho una vez** (sesión MQTT persistente no implementada).
- **Dashboard:** sin hueco en las gráficas, sin contador de descartes, un contador de fallos por mes natural, espacio en blanco en pantalla completa.
- **Puntos de la Fase 2 todavía abiertos:** riesgo de mantenimiento de kafkajs (aceptado), sin seguridad (MQTT anónimo, Kafka PLAINTEXT, sin TLS), sin validación de rangos.

---

## 7. Estado al cierre

- `main` en `0604a08`, CI en verde en los cuatro jobs (ejecución 38496923949). La rama de la Task 3.6 `chore/phase3-publication` está subida: ejecución de CI 38829861901 sobre `8b1caf7` (el último arreglo antes de este informe): pasó tres jobs (lint y tests unitarios: 467 Python, 119 Node.js, "Total coverage: 98.54%", "845/874 branches, 96.68 %"; stack core: 16 pasados, 16 omitidos; pipeline en contenedores: smoke PASS); el job del stack completo solo corre en `main` y en una ejecución manual, así que se omitió.
- Tags: `v0-coursework`, `v1-solid-base`, `v2-engineering-quality`. `v3-quantified-evidence` espera la aprobación del owner, sobre el merge de esta rama.
- Ramas remotas: `main`, y la rama de la Task 3.6 hasta su merge.
- El repositorio es privado. El owner lo hace público, añade la descripción y los temas propuestos en el DETENTE 3.6.a y comprueba la insignia de CI.
- El clon temporal del STEP 0 se creó fuera del árbol de trabajo y se borró después.
