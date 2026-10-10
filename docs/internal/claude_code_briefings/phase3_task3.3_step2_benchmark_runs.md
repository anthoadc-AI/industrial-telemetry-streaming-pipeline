# Task 3.3 — STEP 2: corridas del benchmark del pipeline (F1 a F4)

## Contexto
Rama `feature/phase3-benchmark`, subida a origin, con CI en verde. El STEP 1
está cerrado: arnés, generador, medidor, modos de calibración e informe están
construidos y la corrida de prueba de 30 s es válida. El plan de corridas
aprobado es el del commit 8ae3385 (cinco bloques, 29 corridas esperadas, 36
en el peor caso, entre 4 y 5 horas de portátil enchufado).

Este step ejecuta ese plan. Produce los resultados de los que saldrán las
cifras de F1 a F4. No escribe el README ni decide qué se publica: eso se
decide con Anthony después del último DETENTE.

## Reglas
- No cambies código del pipeline, del arnés ni de `config/` durante este
  step. Si una corrida destapa un defecto, detente y reporta. Un arreglo
  necesita aprobación, y las corridas hechas antes del arreglo se etiquetan
  como tales y no se mezclan con las posteriores.
- Antes de cada corrida, el arnés comprueba corriente alterna, memoria libre,
  contenedores ajenos, suspensión y saltos de reloj. No relajes ninguna
  comprobación. Si una falla, detente y reporta.
- Corrida inválida: se conserva etiquetada, se repite una sola vez y se
  reportan las dos. Dos inválidas seguidas en el mismo punto: detente.
- Ningún resultado se borra ni se edita a mano. Ninguna cifra se escribe a
  mano: todo sale de `make bench-report`.
- Cada manifiesto debe registrar el commit del repo con el que se corrió.
  Comprueba que lo hace antes de la primera corrida; si no, detente.
- Nunca `down -v`. No toques contenedores ajenos al proyecto.
- Un commit de resultados por bloque, con cuerpo narrativo. Push permitido
  tras cada bloque. Sin merge a main.
- Los parámetros (tasas, duraciones, repeticiones, umbrales) son los de
  `config/benchmark.yaml` en 8ae3385. No los ajustes para que una corrida
  pase.

## 2.0 Verificación de estado
1. Rama sincronizada con origin, árbol limpio, último CI en verde.
2. `docker info`: 8 CPU y unos 12,5 GB. Ningún contenedor corriendo.
3. Portátil en corriente alterna. Registra la memoria libre del host.
4. Registra la configuración de suspensión de Windows en corriente alterna
   (`powercfg /query`, suspensión e hibernación). Si el equipo puede
   suspenderse solo durante las próximas horas, detente y reporta: lo ajusta
   Anthony.

## 2.1 Bloque 1: calibración del generador y del medidor
Los modos de calibración no se han ejecutado nunca. Trátalos como código sin
probar: mira la primera corrida entera antes de lanzar la siguiente.
1. Calibración del generador y del medidor, según el plan.
2. Informe: tasa máxima sostenida por cada uno, con el criterio usado, el
   retraso propio del generador en cada tasa y el uso de CPU por contenedor.
3. Si la escalera queda recortada por el generador o el medidor, dilo con la
   tasa de recorte.

### DETENTE 3.3.e
Detente. Reporta los resultados de la calibración, cualquier defecto de los
modos nuevos, y la escalera que resultará (escalones y recorte). No lances el
bloque 2.

## 2.2 Bloques 2 y 3: latencia y escalera
Solo tras la aprobación del DETENTE 3.3.e.
1. Bloque 2: latencia a 5 y a 50 msg/s, 5 repeticiones de cada tasa, en INFO.
2. Bloque 3: escalera de throughput en INFO, una corrida por escalón, y el
   escalón intermedio entre el último que pasa y el primero que falla.
3. Tras cada bloque: `make bench-report`, commit de resultados y push.
4. Vigila la deriva entre repeticiones: si las cifras de una misma tasa
   empeoran de forma monótona de la primera repetición a la última, repórtalo
   como posible efecto térmico, con el uso de CPU por contenedor.

### DETENTE 3.3.f
Detente. Reporta:
- Latencia: por tasa, p50, p95 y p99 (solo si hay muestras suficientes) de
  F1, de `ingest`, de `produce_to_visible`, de F2 y de F4, con la mediana y
  el rango entre repeticiones, y el número de muestras.
- Escalera: por escalón, si pasa o falla y por qué condición (lag, p99 o
  pérdidas), el escalón candidato y el siguiente.
- Qué limita el techo según la evidencia: bridge, consumer, broker, generador
  o medidor.
- Corridas inválidas y su causa. Desvíos. No lances el bloque 4.

## 2.3 Bloques 4 y 5: repeticiones y sensibilidad
Solo tras la aprobación del DETENTE 3.3.f.
1. Bloque 4: repeticiones E2 en el candidato y en el siguiente.
2. Bloque 5: sensibilidad con el bridge en WARNING, en esos dos escalones.
3. `make bench-report` final, commit de resultados y push.

### DETENTE 3.3.g
Detente. Reporta:
- La tabla completa de cifras F1 a F4 tal como sale del informe, cada una con
  su tasa, número de muestras, repeticiones y rango.
- El throughput máximo sostenible con el criterio D8 (p99 menor de 1 s), si
  las repeticiones lo confirman, y el mismo corte recalculado a 100 ms como
  dato interno.
- El costo del log por mensaje: INFO frente a WARNING.
- Tiempo total de portátil, corridas válidas e inválidas.
- Lo que cada cifra no demuestra: limitaciones que deben acompañarla.
- Lo que quedó sin verificar.
No escribas el README. El cierre lo aprueba Anthony.
