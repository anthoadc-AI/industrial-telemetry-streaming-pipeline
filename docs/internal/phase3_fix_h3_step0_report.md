# Corrección H3, STEP 0: diseño (DETENTE H3.a)

Estado: aprobado por Anthony en el DETENTE H3.a (opción (a2), con el contador, el test del alarma fantasma y el test del reloj). Sin cambios en el producto en este paso.

## 1. Estado

Rama `fix/alarm-restore-replay` en `33aecec`, un commit por delante de `main` (`a69859b`): el del briefing. Árbol limpio. Coincide con lo esperado.

## 2. Causa, confirmada

1. **Momento del commit.** kafkajs confirma los offsets cuando vuelven los handlers del lote leído (`processing/alarm_consumer/consumer.js:262`, `autoCommit: true`; comentario del módulo, `consumer.js:12-14`). Si el consumer se para después de enviar una alarma y antes de ese commit, las muestras se vuelven a procesar al arrancar.
2. **Qué reconstruye el arranque.** Solo las alarmas cuyo último evento es RAISED (`consumer.js:197`), que pasan al motor (`processing/alarm_consumer/index.js:166`). `restoreActive` (`state_machine.js:95-99`) pone el estado ACTIVE con los contadores a cero, sin saber cuándo se levantó la alarma. Una alarma cuyo último evento es CLEARED no recibe nada.
3. **El reproceso.** La primera muestra tras el arranque siempre cuenta como continua, porque no hay `seq` anterior (`state_machine.js:155`). Las muestras bajas anteriores al levantamiento cuentan para el borrado (`state_machine.js:79-87`). Eso emite un CLEARED con el `alarm_id` restaurado y el `ts` de la muestra vieja (`state_machine.js:103`, `108`). Después las muestras altas levantan otra vez la misma alarma (`state_machine.js:65-72`).

Reproducido en memoria con el motor y las reglas reales, en un script fuera del repo: CLEARED en seq 2 y otra vez RAISED en seq 7. Coincide con lo que registró el test de integración.

## 3. Segundo defecto en el mismo mecanismo

La sección 6 de `docs/ARCHITECTURE.md` y `docs/internal/phase3_task3.4_h3_result.md:53` decían que una alarma ya borrada que se reprocesa no tiene efecto visible. Solo es cierto si su levantamiento también está en lo reprocesado. Caso no cubierto:

- la alarma se levantó en un lote ya confirmado;
- se borró en un lote sin confirmar, y el consumer se paró después de enviar el CLEARED.

Al arrancar, la reconstrucción ve CLEARED: la alarma empieza NORMAL con los contadores a cero. Las muestras altas reprocesadas la levantan otra vez con un `alarm_id` nuevo y luego la borran. En la simulación: RAISED en seq 12 y CLEARED en seq 17, cuando los originales eran seq 2 y seq 17.

- **En ThingsBoard:** la alarma nueva cuenta como más nueva, así que aparece una alarma fantasma que se borra enseguida.
- **En `telemetry.alarms`:** queda una clave de más.

Lo mismo pasa sin reinicio, si el consumer reprocesa dentro del mismo proceso (la simulación da la misma salida que H3). Es lo que causaría un rebalanceo del grupo; no he verificado que kafkajs reprocese así aquí.

## 4. Opciones

| | Qué cambia | Corrige H3 | Corrige la alarma fantasma | Riesgo principal |
|---|---|---|---|---|
| (a) | Una alarma ACTIVE ignora las muestras con `ts` igual o anterior a su RAISED; la reconstrucción pasa ese `ts`. Solo `state_machine.js`. | sí | **no** (simulado) | si el reloj del productor retrocede, la alarma no se borra hasta que el tiempo pase su RAISED |
| **(a2)** | Toda alarma ignora las muestras con `ts` igual o anterior a su último evento, RAISED o CLEARED. La reconstrucción devuelve también el último CLEARED por tipo de alarma. `state_machine.js` y `readActiveAlarms`. Sin cambio de esquema ni de topic. | sí | sí (simulado) | el mismo riesgo de reloj, que ahora también retrasa levantamientos |
| (b1) | Saltar por `seq` con el `source_seq` del RAISED. | sí | no | **falla**: el simulador reinicia `seq` en 0 (`edge/simulator/__main__.py:197`); tras reiniciarlo, una alarma activa no se podría borrar |
| (b2) | Guardar la posición y los contadores por activo. Topic compactado nuevo o cambio de esquema, y coherencia con el commit de offsets (en la práctica, transacciones). | sí | sí | coste alto y cambio de contrato, que el briefing excluye. Es la única que quita el retraso tras un reinicio |
| (c1) | Commit tras cada mensaje. | solo caídas | solo caídas | menos rendimiento y más latencia de alarma; habría que volver a medir las cifras de la Task 3.3 |
| (c2) | Commit antes y después de cada mensaje que produce una alarma. | solo caídas | solo caídas | un viaje de commit más en la latencia de alarma; mezcla commits manuales con el autocommit de kafkajs, sin verificar |
| (c3) | Descartar el CLEARED, o corregirlo en AlarmMapping. | — | — | ataca el síntoma; el briefing lo excluye |

- Las opciones de commit solo cubren caídas. Un reproceso por cambio de grupo, offsets perdidos o un reset manual sigue produciendo el defecto. (a2) lo cubre porque no depende del motivo del reproceso.
- HI y HIHI del mismo canal: cada una tiene su propio guarda. Con WARNING activa y CRITICAL levantada y borrada en lo reprocesado, (a) repite el RAISED y el CLEARED de CRITICAL con los mismos ids (inocuo); (a2) no emite nada.
- Reinicio durante un backlog grande: (a) y (a2) cuestan una comparación por muestra, sin E/S adicional. (c1) frenaría la puesta al día.
- Retraso de hasta `persistence_samples − 1` muestras tras un reinicio: ni (a), ni (a2), ni (c) lo quitan, porque las muestras de las que depende ya estaban confirmadas y no se reprocesan. Solo (b2). Sigue como limitación conocida.

## 5. Recomendación: (a2)

- **La regla.** Dentro de un tipo de alarma, su último evento ya da cuenta de todas las muestras hasta su `ts`; el motor vuelve a contar a partir de ahí.
- **Prueba.** Aplicado el guarda en memoria (fuera del producto), cinco casos simulados dan el resultado correcto: H3, la alarma fantasma, WARNING con CRITICAL, reproceso seguido del borrado real, y reproceso dentro del mismo proceso.
- **Riesgo de reloj.** Aceptable para un solo host con datos sintéticos; es una limitación nueva y se documenta.
- **Acoplamiento latente en los tests.** `tests/integration/test_thingsboard_sink.py:198` escribe eventos de alarma de vibración de pump-02 con `ts` hasta unos 100 s en el futuro. Con (a2), un consumer arrancado después ignoraría las muestras de vibración de pump-02 durante ese tiempo. Ningún test actual envía alarmas de vibración de pump-02 por el consumer.

## 6. Plan de tests

- Los dos tests de integración existentes, invertidos: el reproceso no emite nada; `telemetry.alarms` con un solo RAISED para la clave hasta el borrado real; ThingsBoard con la alarma activa tras el reproceso y borrada solo tras el borrado real.
- Un test de integración nuevo (núcleo): alarma levantada y confirmada, borrada sin confirmar, offset atrás; el reproceso no emite nada ni deja clave nueva.
- Los tres deben fallar con el código actual y pasar con la corrección.
- Unitarios de la máquina de estados: muestras anteriores, iguales y posteriores al último evento; CLEARED restaurado; HI y HIHI por separado; una transición en vivo también fija el guarda; dos muestras con el mismo `ts`; `ts` que retrocede.
- Unitario del consumer: la reconstrucción devuelve el último evento por tipo de alarma, incluido CLEARED.

## 7. Documentación que cambia

- `docs/ARCHITECTURE.md`: secciones 6, 7, 8, 11, 12 y 13.
- Tabla de trazabilidad: filas 6.8, 6.11, 6.13 y las de las secciones 7, 8, 11 y 13 que mencionan el CLEARED espurio; filas nuevas para el guarda.
- Backlog: H3 cerrado con fecha y commit; la alarma fantasma, encontrada y corregida con él.
- `docs/internal/`: nota breve (problema, acción, resultado) y corrección de `phase3_task3.4_h3_result.md:53`.

## 8. Decisiones de Anthony (DETENTE H3.a)

1. Opción (a2).
2. Contador de muestras saltadas por el guarda, por activo y canal, en `run_finished`.
3. El test de la alarma fantasma entra en esta corrección. Los tres tests de integración deben fallar con el código actual y pasar con la corrección, con las dos ejecuciones a la vista.
4. Riesgo de reloj en la sección 13 y un test unitario que fije el comportamiento cuando el `ts` retrocede antes del último evento (ignorada y contada, nunca borra ni levanta). El acoplamiento se anota en `test_thingsboard_sink.py`.
5. Corregir la frase sobre alarmas borradas en la sección 6 y en `phase3_task3.4_h3_result.md`, y registrar el segundo defecto donde esté el primero: encontrado durante esta corrección, corregido con ella, con su test.
6. Este informe, en `docs/internal/`, en español.
