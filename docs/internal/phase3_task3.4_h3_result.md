# Task 3.4, hallazgo H3: resultado del test de reproceso tras la reconstrucción

Estado (2026-10-13): **defecto corregido**, junto con un segundo defecto encontrado durante la corrección; ver `docs/internal/phase3_fix_h3_result.md`. Los tests de `tests/integration/test_restore_replay.py` ya no fijan el defecto: exigen el comportamiento correcto. Lo que sigue es el resultado del 2026-10-12, sin cambios salvo la corrección de la sección "Condiciones y alcance".

Estado original: resultado verificado, antes de escribir la sección 6 ("Alarm logic") de `docs/ARCHITECTURE.md`, como pidió Anthony en la aprobación del DETENTE 3.4.a. Sin cambios en el producto.

## Pregunta

Una alarma restaurada como ACTIVE y después un reproceso de muestras anteriores a su RAISED que cumplen la condición de borrado: ¿emite el alarm consumer un CLEARED espurio? ¿Qué llega a `telemetry.alarms` y a ThingsBoard después?

## Cómo se produce ese estado

- El consumer confirma los offsets de `telemetry.raw` con el autocommit de kafkajs, cuando han vuelto los handlers del lote leído (`processing/alarm_consumer/consumer.js`, `runConsumer`, `autoCommit: true`).
- Si se para entre el envío de un RAISED y ese commit, el RAISED queda en `telemetry.alarms` y el offset confirmado queda antes de las muestras que lo levantaron. Puede pararse por un crash, por un kill o por la parada documentada cuando falla un envío más adelante en el lote.
- Al arrancar, `restoreActive` (`state_machine.js`) marca la alarma ACTIVE con los contadores a cero. Después el consumer vuelve a procesar el lote desde el principio, incluidas las muestras normales anteriores al RAISED.

## Test

`tests/integration/test_restore_replay.py`. El test construye ese estado sin cronometrar un crash:

1. El consumer A procesa 5 muestras normales y 3 altas de `pump-02`, `bearing_temperature_c` (WARNING 80, histéresis 3, persistencia 3), y levanta una alarma.
2. El offset confirmado del grupo en `telemetry.raw` se mueve a antes de esas muestras.
3. El consumer B arranca en el mismo grupo.
4. Unas muestras normales posteriores borran la alarma, para no dejar nada activo.

Hay dos tests:
- `test_replay_before_the_raise_emits_a_spurious_clear` (núcleo, corre en el job `integration` de CI): comprueba lo que emite el consumer y lo que queda en `telemetry.alarms`.
- `test_after_the_replay_thingsboard_shows_the_alarm_cleared_while_it_is_active` (perfil `full`): reenvía esos eventos con el sink y comprueba el estado de la alarma en ThingsBoard.

## Resultado (2026-10-12, sesión local con perfil `full`)

Pasaron los 2 tests en 141 s. Al cerrar la sesión: cola PipelineSequential con 20 mensajes y 0 descartados, 0 fallos de mapeo, 0 alarmas activas y ningún volumen de más.

**El defecto existe.** Lo que registró el consumer B en la ejecución con ThingsBoard:

| Evento | alarm_id | source_seq | offset |
|---|---|---|---|
| `alarms_restored` | (1 activa) | — | — |
| `alarm_cleared` | `pump-02/bearing_temperature_c/WARNING/1791853827186` | …188, la 3.ª muestra normal | 13 |
| `alarm_raised` | el mismo | …193, la última muestra alta | 18 |

1. **El consumer emite un CLEARED espurio** con el `alarm_id` restaurado. El `ts` del CLEARED es anterior al de la muestra que levantó la alarma. Después emite el mismo RAISED otra vez.
2. **En `telemetry.alarms`** queda, para esa clave, RAISED, CLEARED y RAISED. El último registro de la clave es RAISED, así que una reconstrucción posterior acierta. Un lector que recorra el topic ve un borrado que no ocurrió.
3. **En ThingsBoard** la alarma queda borrada mientras la condición sigue activa.
   - AlarmMapping aplica el CLEARED porque lleva el `raised_ts` de la alarma que tiene en RAISED.
   - Descarta el segundo RAISED como duplicado, porque no es más nuevo.
   - Cuando la condición se borra de verdad, también descarta ese CLEARED.
   - Resultado: ThingsBoard no muestra la alarma durante el resto de su duración. El test lo comprueba: `CLEARED_UNACK` tras el reproceso y también tras el borrado real.

## Condiciones y alcance

- Hace falta que el tramo sin confirmar contenga, antes de las muestras que levantaron la alarma, al menos `persistence_samples` muestras seguidas del mismo activo por debajo de umbral − histéresis.
- Con consumo en vivo los lotes suelen ser pequeños. Al ponerse al día con un backlog, por ejemplo tras un reinicio, los lotes son grandes.
- No he medido con qué frecuencia ocurre. El test fuerza el estado moviendo el offset, no matando el proceso a mitad de lote.
- ~~El caso contrario no tiene efecto visible~~ **Corregido el 2026-10-13:** esa afirmación solo vale si el levantamiento de la alarma también está en lo reprocesado; entonces se repiten RAISED y CLEARED con el mismo `alarm_id` y ThingsBoard los descarta. Si la alarma se levantó en un lote ya confirmado y se borró en uno sin confirmar, la reconstrucción la deja NORMAL y el reproceso de sus muestras altas la levanta otra vez con un `alarm_id` nuevo y la borra: una alarma fantasma en ThingsBoard y una clave de más en `telemetry.alarms`. Se encontró durante la corrección de H3 (`docs/internal/phase3_fix_h3_step0_report.md`, sección 3) y se corrigió con ella (`docs/internal/phase3_fix_h3_result.md`).

## Qué se hace con él (decisión de Anthony)

- `docs/ARCHITECTURE.md` lo escribe como defecto conocido en "Alarm logic" y en "Known limitations".
- Elemento de backlog en `docs/internal/phase3_backlog.md`, marcado "before the repository goes public".
- El test se queda. Fija el comportamiento actual: cuando se arregle el consumer, el test falla y hay que cambiarlo junto con el documento y el elemento de backlog.
