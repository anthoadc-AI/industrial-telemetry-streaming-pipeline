# Corrección H3: resultado (para el informe de cierre de la Fase 3)

Rama `fix/alarm-restore-replay`. Corrección en `4b87a8d`, tests en `9a164b8`. Diseño y alternativas: `docs/internal/phase3_fix_h3_step0_report.md`.

## Problema

Dos defectos del mismo mecanismo en el alarm consumer. kafkajs confirma los offsets de `telemetry.raw` cuando vuelven los handlers del lote. Si el consumer se para entre el envío de una alarma y ese commit, al arrancar reconstruye el estado desde `telemetry.alarms` y vuelve a evaluar las muestras que ya causaron el evento.

1. **CLEARED espurio (H3, encontrado en la revisión de arquitectura, Task 3.4).** Una alarma restaurada como ACTIVE se borraba con muestras anteriores a su RAISED y se volvía a levantar con el mismo `alarm_id`. ThingsBoard la mostraba borrada mientras la condición seguía activa (`docs/internal/phase3_task3.4_h3_result.md`).
2. **Alarma fantasma (encontrado durante esta corrección).** Una alarma levantada en un lote confirmado y borrada en uno sin confirmar se restauraba como NORMAL; el reproceso de sus muestras altas la levantaba con un `alarm_id` nuevo y la borraba. ThingsBoard mostraba una alarma de más y `telemetry.alarms` guardaba una clave de más. `docs/ARCHITECTURE.md` y la nota de H3 decían que este caso no tenía efecto visible; las dos afirmaciones están corregidas.

## Acción

- **Guarda de reproceso** (`processing/alarm_consumer/state_machine.js`). Cada alarma de nivel guarda el `ts` de su último evento, RAISED o CLEARED, emitido o restaurado. Una muestra con `ts` igual o anterior no cuenta para esa alarma: no cambia el estado ni los contadores. Un corte de `seq` sigue poniendo los contadores a cero, también en las muestras saltadas.
- **Reconstrucción** (`readAlarmState`, antes `readActiveAlarms`, en `consumer.js`). Devuelve el último evento por tipo de alarma (activo, canal, severidad), incluido CLEARED. `restoreFrom` sustituye a `restoreActive`.
- **Contador.** `samples_skipped_by_replay_guard`, por activo y canal, en `run_finished`.
- Sin cambios en `alarm_v1` ni en el contrato de `telemetry.alarms`.
- **Coste aceptado (decisión de Anthony, DETENTE H3.a).** Si el reloj de un productor retrocede, la alarma ignora esas muestras hasta que el tiempo pase su último evento. Fijado con un test unitario y documentado en la sección 13 de `docs/ARCHITECTURE.md`.

## Resultado

### Tests de integración: fallan con el código anterior, pasan con la corrección

`tests/integration/test_restore_replay.py`, con la versión final de los tests (`9a164b8`).

**Con el código anterior** (`processing/alarm_consumer/` igual que `main`, `a69859b`). Cada test en su propia sesión de pytest, con un stack nuevo, para que el fallo de uno no contamine al siguiente:

| Test | Perfil | Resultado | Aserción que falla |
|---|---|---|---|
| `test_replay_before_the_raise_does_not_clear_the_restored_alarm` | core | falla | `transitions(b) == []`: el consumer B emitió 2 eventos, el primero `alarm_cleared` |
| `test_replay_of_a_clear_does_not_raise_a_phantom_alarm` | core | falla | `transitions(b) == []`: el consumer B emitió 2 eventos, el primero `alarm_raised` |
| `test_after_the_replay_thingsboard_keeps_the_alarm_active_until_the_real_clear` | full | falla | ThingsBoard muestra `CLEARED_UNACK` donde se exige `ACTIVE_UNACK` |

**Con la corrección** (`4b87a8d`) y los mismos tests: los tres pasan en una sesión `full` (3 passed en 139 s), y también dentro de las suites completas de abajo.

### Suites completas en local, con la corrección

- Unitarios: 446 Python y 119 Node; cobertura del consumer en Node: 100 % de líneas, 98,7 % de ramas.
- Integración core (`make test-integration`): 15 passed, 17 skipped.
- Integración full (`make test-integration-full`): 31 passed, 1 skipped (el test de SIGTERM, solo Linux), en 1024 s. Al cerrar la sesión: cola PipelineSequential con 10024 mensajes, 0 descartados, 0 reintentados, 0 fallos de mapeo, 0 alarmas activas, ningún volumen de más.

## Desvíos e incidencias

- **Ejecuciones con el código anterior antes de la versión final de los tests.** Las hice con la versión previa a `start_ms` (abajo); la tabla de arriba es la repetición con la versión final.
- **Primera ejecución con el código anterior, los tres tests en una sesión.** Solo el primero falló por su motivo; los otros dos fallaron en su preparación, porque el primero se paró antes de borrar su alarma y la dejó activa. Por eso cada test se ejecutó después en su propia sesión.
- **Primera ejecución del test de ThingsBoard por separado.** Falló al arrancar el sink por `TB_GATEWAY_TOKEN`: llamé a pytest sin exportar `.env`, que `make test-integration-full` exporta. Repetido con `.env` exportado.
- **Primera ejecución con la corrección.** El test de la alarma fantasma falló en su preparación: el test anterior del mismo fichero estampa sus muestras hasta unos 10 s por delante del reloj, y el guarda (correctamente) ignoró las muestras del siguiente. Es el acoplamiento por `ts` previsto en el STEP 0, dentro del propio fichero. Arreglado en los tests: cada uno empieza después del último evento de su tipo de alarma en `telemetry.alarms` (`start_ms`).
- **Test unitario de la Task 2.2.** Reutilizaba los mismos `ts` antes y después del reinicio simulado. Con el guarda, su fase F3 borraba en la muestra 9 y no en la 4. Ahora el reloj avanza entre fases, como en la realidad; las aserciones no cambian.

## Sin verificar

- Con qué frecuencia se daban los dos defectos en la práctica: no se ha medido.
- Que kafkajs reprocese dentro del mismo proceso tras un rebalanceo: el caso está cubierto por un test unitario del guarda, pero no se ha provocado un rebalanceo real.
- El retraso de hasta `persistence_samples − 1` muestras tras un reinicio no cambia (backlog).
