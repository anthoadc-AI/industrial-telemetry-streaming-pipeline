# Task 3.3 — STEP 1: implementación del benchmark (sin corridas publicables)

## Contexto
Rama `feature/phase3-benchmark` en 78673d2, tres commits sobre main (a895bb3).
El STEP 0 está cerrado: la propuesta está en
`docs/internal/benchmark_protocol_proposal.md`.

Anthony confirmó el 2026-10-09 la aprobación del DETENTE 3.3.a, con D1 a D9
según la recomendación y con las enmiendas y la decisión siguientes. Donde
este briefing y la propuesta difieran, gana este briefing.

### Enmiendas aprobadas
- E1 (sobre D1). Bridge, alarm consumer y sink pasan a correr en contenedores
  como un perfil del compose. Es un cambio de producto, no un apaño del
  benchmark: debe quedar utilizable para cualquiera que clone el repo. Las CPU
  de la VM se fijan y se declaran antes de medir.
- E2 (sobre D5). El techo de throughput se busca con una corrida por escalón.
  Las cinco repeticiones se hacen solo en el escalón candidato y en el
  siguiente.
- E3 (sobre D6 y D7). Antes de construir nada se verifica (a) si bridge y
  alarm consumer aceptan activos que no están en la config y (b) si el
  observador por WebSocket de ThingsBoard funciona. Si (b) falla, la cifra F6
  (latencia hasta ThingsBoard) se elimina; no se improvisa otro método.
- E4 (texto). En la propuesta: el desvío sobre Airflow y Hadoop debe decir que
  esos contenedores se bajaron el 2026-10-05 con
  `docker compose -p <proyecto> down`, sin `-v`, con sus volúmenes
  conservados; y H1 debe decir "ubicación confirmada (camino host a VM),
  mecanismo sin identificar".

### Decisión D8
Criterio de throughput máximo sostenible: p99 extremo a extremo menor de 1 s,
justificado como un periodo de muestreo del sensor, más lag acotado y cero
pérdidas. Es la única cifra de throughput que se publica. Los resultados
crudos deben permitir recalcular el corte con otros umbrales sin repetir
corridas.

### D2
El bridge conserva `linger.ms` por defecto. El arnés debe poder lanzar una
corrida con otro valor por parámetro, sin tocar el código del bridge, para una
posible corrida de sensibilidad que se decidirá después.

## Reglas
- Este step construye y prueba. No produce cifras publicables. Las corridas
  reales son el STEP 2, con briefing propio.
- No hagas push ni merge. Si necesitas CI, pídelo en el DETENTE.
- No modifiques `.wslconfig` ni nada fuera del repo. Si hace falta un cambio
  ahí, propónlo y Anthony lo aplica.
- No bajes ni borres contenedores ajenos al proyecto. Nunca `down -v` sin
  autorización explícita.
- Antes de cualquier ejecución que mida algo: corriente alterna y `docker ps`
  sin contenedores ajenos. Si falla alguna, detente y reporta.
- Fuente única de verdad: todo parámetro del benchmark (tasas, duraciones,
  umbrales, repeticiones) vive en `config/`, no en los scripts.
- Camino C: inglés en código, comentarios y commits; español solo en
  `docs/internal/`.
- Conventional Commits con cuerpo narrativo. Un commit por unidad lógica.
- Si el código contradice este briefing, gana el código: repórtalo como
  desvío.
- Los tests existentes (293 Python, 108 Jest, 12 de integración core, 25 del
  perfil full) deben seguir pasando. Verifica esas cifras tú; no las tomes de
  aquí.

## 1.0 Verificación de estado y enmiendas
1. Rama en 78673d2, árbol limpio, Docker sin contenedores ajenos corriendo.
2. Aplica E4 a la propuesta y añade una sección "Enmiendas del 2026-10-09"
   con E1 a E3 y la decisión D8. Commit propio.

## 1.1 Verificaciones previas (E3) y diseño
1. Activos sintéticos: determina leyendo el código, y con una prueba mínima,
   qué hacen bridge y alarm consumer con un `asset_id` que no está en la
   config. Si lo rechazan, propón cómo generar N activos para throughput sin
   debilitar la validación del producto.
2. Observador WebSocket: prueba de concepto acotada a 60 minutos de trabajo.
   Debe demostrar que recibe cada valor de telemetría de un dispositivo con
   su marca de tiempo, sin perder ninguno, a 10 msg/s. Si no lo logra en ese
   tiempo, repórtalo y F6 se elimina.
3. Diseño de la contenerización (E1), sin implementarla todavía:
   - Dockerfiles, imágenes base con versión fijada, usuario no root.
   - Nombre del perfil del compose y qué hace `make up` por defecto.
   - Cómo conviven los procesos en el host (flujo actual de desarrollo y de
     los tests de integración) con los servicios en contenedor.
   - Impacto en CI: tiempo de build y si los jobs actuales cambian.
   - Reparto de CPU: cuántas CPU necesita la VM, qué límites por servicio
     propones, y qué valor de `processors` recomiendas para `.wslconfig`.
4. Diseño del generador y del arnés, sin implementarlos todavía: módulos,
   formato del archivo de resultados, cómo se calcula cada cifra F1 a F7,
   comprobaciones de validez (corriente, suspensión, contenedores ajenos) y
   de integridad.

### DETENTE 3.3.b
Detente. Reporta los resultados de 1 y 2 con evidencia, y los diseños de 3 y
4 con las decisiones que necesita Anthony. No implementes nada todavía.

## 1.2 Contenerización del pipeline
Solo tras la aprobación del DETENTE 3.3.b, y según lo aprobado.
1. Implementa los Dockerfiles y el perfil del compose.
2. `make` debe ofrecer un objetivo que levante el pipeline completo en
   contenedores y un smoke que lo verifique (equivalente al 200 de 200
   actual).
3. Tests: los existentes siguen pasando; añade los de integración que
   justifique el cambio.
4. `check-secrets` limpio. Ninguna credencial en imágenes ni en el compose.

### DETENTE 3.3.c
Detente. Reporta: qué cambió en el compose y en el Makefile, salida del smoke
en contenedores, resultado de todos los tests, tamaño de las imágenes y
desvíos. Indica si recomiendas subir la rama para correr CI.

## 1.3 Generador de carga y arnés de medición
Solo tras la aprobación del DETENTE 3.3.c.
1. Generador en lazo abierto: cada envío programado en `t0 + i / tasa` sobre
   reloj monotónico; `ts` del mensaje igual al instante programado; el retraso
   propio del generador se registra aparte.
2. Arnés: orquesta una corrida (recrear stack, calentamiento, intervalo
   medido, recogida), valida el entorno antes y después, comprueba la
   integridad (enviados = recibidos = registros nuevos en el topic, DLQ
   contada) e invalida la corrida si algo no cuadra.
3. Resultados: un directorio por corrida en `bench/results/`, legible por
   máquina, con entorno, parámetros, muestras crudas o histograma de
   resolución fija, contadores e indicador de validez.
4. Script que genera las tablas de cifras a partir de los resultados. Ninguna
   cifra se escribe a mano.
5. Tests unitarios del generador (planificación, sin omisión coordinada), del
   cálculo de percentiles y del criterio de throughput sostenible, con
   cobertura dentro de los umbrales del repo.
6. Corrida de prueba corta (30 s medidos, una tasa baja) para demostrar que
   el arnés funciona de punta a punta. Sus cifras no son publicables y deben
   quedar etiquetadas como tales.
7. Plan de corridas del STEP 2: lista de puntos, tasas de la escalera,
   repeticiones según E2 y tiempo total estimado de portátil enchufado.

### DETENTE 3.3.d
Detente. Reporta: lo construido, resultado de los tests, salida de la corrida
de prueba, el plan de corridas con su duración estimada, desvíos y lo que
quedó sin verificar. No lances ninguna corrida real. El cierre lo aprueba
Anthony.
