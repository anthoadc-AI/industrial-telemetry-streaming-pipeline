# Fase 3 — Corrección del defecto H3: CLEARED espurio tras reconstrucción y reproceso

## Contexto
main en a69859b, CI verde en los cuatro jobs. Tasks 3.3 y 3.4 cerradas.
El defecto está descrito en `docs/internal/phase3_task3.4_h3_result.md`, en la
sección 6 de `docs/ARCHITECTURE.md` y en el backlog, marcado "before the
repository goes public". Dos tests lo reproducen y fijan el comportamiento
defectuoso: `tests/integration/test_restore_replay.py`.

Resumen del defecto: tras un reinicio, el alarm consumer restaura una alarma
como ACTIVE y luego reprocesa muestras anteriores a su RAISED. Si entre ellas
hay `persistence_samples` muestras bajo el nivel de borrado, emite un CLEARED
con `ts` anterior al RAISED. ThingsBoard lo aplica, descarta el RAISED
repetido y descarta también el CLEARED real: la alarma desaparece de la vista
mientras la condición sigue activa.

Esta corrección se hace antes del README (Task 3.5) por decisión de Anthony.

## Reglas
- Rama `fix/alarm-restore-replay` desde main. Sin merge hasta la aprobación.
- Reconstruir antes que parchear: la corrección debe atacar la causa (el
  reproceso de historia anterior al RAISED), no el síntoma en ThingsBoard.
- Cambio mínimo en el producto, acotado al alarm consumer. No se cambia el
  esquema `alarm_v1` ni el contrato de `telemetry.alarms` salvo que el STEP 0
  demuestre que no hay alternativa; en ese caso, se propone y se detiene.
- Los dos tests existentes dejan de fijar el defecto y pasan a exigir el
  comportamiento correcto. No se borran: se invierten.
- Camino C: código, comentarios, commits y `docs/ARCHITECTURE.md` en inglés;
  notas en `docs/internal/` en español.
- Nada se afirma sin evidencia: el resultado se demuestra con los tests, en
  local y en CI, incluido el perfil full.

## STEP 0 — Diseño (sin implementar)
1. Estado: main en a69859b, árbol limpio, rama creada.
2. Lee `state_machine.js`, `consumer.js` (`restoreActive`, `readActiveAlarms`,
   `runConsumer`) y los dos tests de reproceso. Confirma la causa con
   referencias a línea.
3. Evalúa como mínimo estas opciones, con costo, riesgo y qué cubren:
   - (a) Guarda de reproceso en la máquina de estados: una alarma restaurada
     como ACTIVE conoce el `ts` de su RAISED (está en el `alarm_id`). Una
     muestra con `ts` igual o anterior a ese valor no cuenta para el contador
     de borrado de esa alarma ni cambia su estado. Las muestras posteriores
     se procesan con normalidad; un CLEARED repetido lleva el mismo `alarm_id`
     y es inocuo.
   - (b) Reconstruir también la posición: guardar por activo el último `seq`
     procesado y saltar en el reproceso las muestras ya vistas.
   - (c) Cualquier otra que encuentres, incluido cambiar el momento del
     commit de offsets.
   Para cada una: qué pasa con el caso contrario (alarma ya borrada y
   reprocesada), con dos alarmas del mismo canal (HI y HIHI), con un
   reinicio durante un backlog grande, y qué efecto tiene en la segunda
   limitación de H3 (el retraso de hasta `persistence_samples − 1` muestras).
4. Recomienda una opción. Mi criterio, no vinculante: (a) parece la menor y
   la más localizada; dime si ves un caso en que falle.
5. Propón los tests: los dos existentes invertidos, tests unitarios de la
   máquina de estados para el guarda (muestra anterior, igual y posterior al
   RAISED; HI y HIHI; caso contrario) y qué cambia en las aserciones del test
   con ThingsBoard.
6. Lista qué secciones de `docs/ARCHITECTURE.md`, de la tabla de
   trazabilidad y del backlog cambian.

### DETENTE H3.a
Detente. Reporta la causa confirmada, las opciones con su evaluación, tu
recomendación, el plan de tests y los cambios de documentación. No
implementes nada.

## STEP 1 — Corrección
Solo tras la aprobación del DETENTE H3.a, y según lo aprobado.
1. Implementa la corrección en el alarm consumer, en su propio commit, con
   cuerpo narrativo que explique la causa y por qué esta opción.
2. Tests unitarios nuevos; cobertura dentro de los umbrales del repo.
3. Invierte los dos tests de integración para que exijan: ningún CLEARED con
   `ts` anterior al RAISED, `telemetry.alarms` con un solo RAISED para la
   clave hasta el borrado real, y ThingsBoard mostrando la alarma activa tras
   el reproceso y borrada solo tras el borrado real.
4. Documentación, en commit aparte: `docs/ARCHITECTURE.md` (secciones 6, 7,
   8, 11 y 13: el defecto pasa de "conocido y abierto" a "encontrado en la
   revisión de arquitectura, corregido, con el test que lo demuestra"),
   tabla de trazabilidad, backlog (elemento cerrado con fecha y commit) y
   una nota breve en `docs/internal/` (español) con el resultado, en formato
   problema-acción-resultado, para el informe de cierre de la Fase 3.
5. Comprueba que el test de sincronía del documento sigue pasando.
6. Unit, integración core y full en local, push, CI verde con dispatch
   manual de integration-full.

### DETENTE H3.b
Detente. Reporta: el diff del producto, los resultados de los tests antes y
después (los dos de reproceso deben fallar con el código viejo y pasar con el
nuevo; demuéstralo), CI, desvíos y lo que quedó sin verificar. Sin merge. El
cierre lo aprueba Anthony.
