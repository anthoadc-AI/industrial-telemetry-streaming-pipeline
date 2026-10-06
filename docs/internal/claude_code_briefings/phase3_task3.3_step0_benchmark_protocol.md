# Task 3.3 — STEP 0: diseño del protocolo de medición (benchmark)

## Contexto
main en a895bb3, CI verde en los tres jobs. Tasks 3.1 y 3.2 cerradas.
Este STEP 0 produce un protocolo de medición, no cifras. Ninguna cifra que
salga de aquí es publicable. No se ejecuta ningún benchmark hasta que
Anthony apruebe el DETENTE 3.3.a.

## Reglas
- Rama `feature/phase3-benchmark` desde main. Nada se mergea en este step.
- No modificar `src/`, `config/` ni `docker-compose.yml`. Los scripts de
  diagnóstico van en `scripts/bench/diagnostics/` y son desechables.
- Si algo del briefing contradice lo que encuentras en el código, gana el
  código: repórtalo como desvío, no lo adaptes en silencio.
- Cada afirmación del reporte lleva su evidencia (comando, salida, archivo
  y línea). Lo que no verificaste se etiqueta "no verificado".

## 0. Verificación de estado
1. `git status` limpio, rama `feature/phase3-benchmark` creada desde
   a895bb3, último run de CI en main verde.
2. Ningún contenedor ajeno corriendo (Airflow y Hadoop de otros cursos
   resucitan con Docker Desktop). Si los hay, repórtalo y detente: no los
   bajes tú.
3. Registra el entorno: CPU (modelo, núcleos), RAM, `.wslconfig`, versión de
   Docker Desktop y backend, versión del kernel de la VM, plan de energía de
   Windows, versiones de las imágenes del compose, versiones de los clientes
   Kafka (Python y Node) y su configuración efectiva de productor
   (linger, batch, acks, nagle, compresión).
4. Inventario de lo que ya se mide: cada métrica de latencia existente
   (`produce_to_ack_ms`, `ts_to_ack_ms` y cualquier otra), dónde se toma cada
   timestamp, con qué reloj (wall o monotónico) y en qué proceso.

## 1. Pregunta 1 — la discrepancia de produce_to_ack_ms
Observado: ~16 ms en el smoke, ~42-44 ms con mensajes aislados.
Hipótesis a discriminar (ninguna está verificada):
- H1 reenvío de puertos de Docker Desktop (host Windows a la VM).
- H2 Nagle + delayed ACK (una diferencia de ~40 ms es su firma típica).
- H3 linger/batching: un mensaje aislado espera el linger completo.
- H4 conexión o CPU en reposo entre mensajes aislados.
- H5 cadencia del bucle de poll del productor.
- H6 resolución del temporizador de Windows (~15,6 ms).

Diseña y ejecuta la matriz mínima que las separe:
- Ubicación del cliente: host Windows / contenedor en la red del compose
  (este es el primer experimento) / distro WSL si aplica.
- Cadencia: aislado (1 msg/s) y ráfaga (la del smoke).
- Para H2 y H3: variar solo `socket.nagle.disable` y linger, de uno en uno.
- N >= 200 por celda, descartando los primeros 20. Reporta p50, p95, min y
  max por celda, con los datos crudos en CSV.
Entrega: tabla de la matriz y, por hipótesis, confirmada / descartada /
indeterminada con la celda que lo sustenta. Si queda indeterminada, dilo.

## 2. Pregunta 2 — ubicación de clientes y relojes
Analiza y recomienda, con evidencia:
- Qué mediciones son de un solo proceso (deben usar reloj monotónico) y
  cuáles cruzan procesos (dependen de un reloj de pared común).
- Mide el desfase entre el reloj del host Windows y el de la VM de Docker
  (varias muestras, y de nuevo tras 10 minutos, para ver deriva).
- Verifica que los contenedores comparten el reloj del kernel de la VM.
- Propón dónde corren generador de carga y medidor. Evalúa de forma
  explícita la opción "todo en contenedores dentro de la misma VM" frente a
  "clientes en el host", con lo que cada una mide y lo que cada una sesga.
- El generador de carga compite por CPU con el sistema bajo prueba en la
  misma máquina: propón cómo detectarlo (uso de CPU del generador, cuotas).

## 3. Pregunta 3 — Kafka en tmpfs
- Confirma en el compose qué vive en tmpfs y con qué límites.
- Calcula el volumen de datos de una corrida: bytes por mensaje en el log x
  tasa x duración, frente a la retención y a los 12 GB de WSL.
- Compara tres opciones con sus costos: (A) mantener tmpfs y declararlo,
  (B) volumen con nombre solo para el benchmark vía override de compose,
  dejando CI en tmpfs, (C) medir ambas y reportar la diferencia.
- Indica qué cambia de verdad con (B) dada la política de flush del broker
  (page cache, sin fsync por mensaje) y que el volumen vive en el VHDX.
No cambies el compose. Solo recomienda.

## 4. Pregunta 4 — camino a ThingsBoard
El camino a ThingsBoard se mide y reporta aparte del pipeline.
- Define con precisión dónde termina "pipeline" (propuesta: alarma
  confirmada en `telemetry.alarms`) y dónde empieza "sink ThingsBoard".
- Propón si el benchmark del pipeline corre en perfil core (sin ThingsBoard
  consumiendo recursos) y el del sink en perfil full.
- Para el sink: qué se mide (tasa sostenida al límite configurado de
  10 msg/s por activo, latencia hasta ThingsBoard, comportamiento por
  encima del techo: retraso acumulado, pérdidas = 0 verificable) y cómo se
  observa cada cosa.
- Verifica en el código y la config los valores actuales del límite y de los
  retardos SQL; no los tomes de este briefing.

## 5. Pregunta 5 — cifras candidatas a publicación
Propón la lista de cifras candidatas para el README. Por cada una:
definición exacta, puntos de medición, unidad, y qué limitación debe
acompañarla en el texto. El protocolo debe cubrir como mínimo:
- Carga en lazo abierto a tasa fija, con latencia medida desde el instante
  de envío previsto (evitar omisión coordinada).
- Calentamiento descartado; duración mínima por corrida; número de
  repeticiones; percentiles calculados sobre muestras crudas, nunca
  promediando percentiles; p99 solo si el número de muestras lo sostiene.
- Criterio operativo de "throughput máximo sostenible" (lag estable,
  latencia acotada, cero pérdidas), no la tasa más alta que no se cayó.
- Verificación de integridad en cada corrida: mensajes enviados =
  recibidos, DLQ contado.
- Resultados en archivos legibles por máquina versionados en el repo, y las
  cifras del README generadas a partir de ellos.
- Declaración del entorno: una sola máquina, Docker Desktop sobre WSL 2,
  un broker, datos sintéticos. No es una cifra de capacidad de producción.

## Entregable
`docs/internal/benchmark_protocol_proposal.md` (español) con las secciones
0 a 5, más una sección "Decisiones que necesita Anthony": cada una con
opciones, recomendación y costo de cada opción.

## DETENTE 3.3.a
Detente aquí. Reporta: (1) resumen de hallazgos por pregunta, (2) lista de
decisiones pendientes, (3) desvíos respecto a este briefing, (4) lo que
quedó sin verificar. No implementes el benchmark, no cambies el compose, no
hagas commit fuera de la rama. El cierre lo aprueba Anthony.
