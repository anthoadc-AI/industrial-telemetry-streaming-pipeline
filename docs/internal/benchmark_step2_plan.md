# Task 3.3 — Plan de corridas del STEP 2

Estado: actualizado con las correcciones del DETENTE 3.3.d. Generado a partir de `config/benchmark.yaml` con `make bench-plan` (`bench/analysis.py: plan`). Ninguna corrida de este plan se ha lanzado.

Los tiempos son estimaciones basadas en la corrida de prueba del STEP 1 (133 s en total para 60 s de envío). La duración de la calibración del medidor supone 10 000 registros/s para cargar y para leer, y es una hipótesis, no una medida.

**Alcance:** solo F1 a F4. F5 y F6 tendrán un modo reducido del arnés en un step posterior; F7 está en el backlog, porque ThingsBoard ignora SIGTERM.

## Condiciones de cada corrida

- **Entorno:** corriente alterna, ningún contenedor de otro proyecto y al menos 1,5 GB libres en el host. El arnés lo comprueba antes de empezar, cada 5 s durante la corrida y al final, e invalida la corrida si algo falla. También detecta suspensiones y saltos de reloj.
- **Stack:** se recrea en cada corrida. El pipeline corre en contenedores, con las vCPU fijadas por servicio (D-E).
- **Pipeline:** 30 s de calentamiento, 300 s medidos, drenaje y **120 s de enfriamiento** entre corridas. Se registra el uso de CPU por contenedor.
- **Bridge:** en INFO, el valor por defecto del producto, salvo en el bloque de sensibilidad.
- **Generador saturado:** la corrida es inválida y el escalón se informa como "limitado por el generador".

## Bloques, en este orden

| # | Bloque | Corridas | Detalle |
|---|---|---|---|
| 1 | **Calibración** | del generador: una por tasa (1000, 2000, 4000, 6000, 8000 y 12 000 msg/s, 60 s medidos), parando en la primera que no sostiene; del medidor: 1 (600 000 registros precargados) | Fija el tope de la escalera: la menor de las dos tasas máximas. Si la escalera lo supera, se recorta ahí y el informe lo indica. |
| 2 | **Latencia** | 5 msg/s × 5 y 50 msg/s × 5 | Los 5 activos reales, bridge en INFO. F1 (con `ingest` y `produce_to_visible`), F2 y F4. |
| 3 | **Escalera** | una por escalón de 250, 500, 1000, 2000, 3000, 4000, 6000 y 8000 msg/s (recortada al tope de la calibración), **más un escalón intermedio** entre el último que pasa y el primero que falla | 100 clones (D-A), bridge en INFO. Se detiene en el primer escalón que falla. |
| 4 | **Repeticiones E2** | 4 más en el escalón candidato y 4 en el siguiente | Confirman F3 (D8). |
| 5 | **Sensibilidad del log** | el candidato y el siguiente, 1 vez cada uno, con el bridge en WARNING | Cuantifica el coste del log por mensaje frente a la escalera en INFO. |

## Duración (portátil enchufado)

Corrida del pipeline: 540 s. Calibración del generador: 255 s por tasa. Calibración del medidor: unos 315 s (estimado).

| Bloque | Caso esperado | Peor caso |
|---|---|---|
| 1 Calibración | 4 corridas, 0,30 h | 7 corridas, 0,51 h |
| 2 Latencia | 10 corridas, 1,50 h | 10 corridas, 1,50 h |
| 3 Escalera y escalón intermedio | 5 corridas, 0,75 h | 9 corridas, 1,35 h |
| 4 Repeticiones E2 | 8 corridas, 1,20 h | 8 corridas, 1,20 h |
| 5 Sensibilidad | 2 corridas, 0,30 h | 2 corridas, 0,30 h |
| **Total** | **29 corridas, 4,05 h** | **36 corridas, 4,86 h** |

- **Caso esperado:** la calibración del generador se detiene a mitad de sus tasas y la escalera falla a mitad de sus escalones.
- **Peor caso:** se corren todas las tasas de calibración y todos los escalones, y falla el último.
- **Sesiones:** cada corrida es independiente y escribe su propio directorio en `bench/results/`. Propongo hacer cada bloque entero en una misma sesión, para no mezclar condiciones térmicas.

## Desvíos de la implementación

1. **El generador programa sobre el reloj de pared de la VM, no sobre el monotónico.** En la VM, `CLOCK_MONOTONIC` y `CLOCK_REALTIME` se separan unos 100 ppm (−94 ppm medidos en la prueba). Todos los timestamps de la corrida usan el reloj de pared.
2. **La pendiente del lag admite 0,001 s/s, no 0**, y **D8 solo se evalúa en las corridas de throughput**. Las dos cosas constan para el informe de cierre (`docs/internal/phase3_backlog.md`).
3. **Las funciones de E/S del arnés llevan `pragma: no cover`**. También consta para el informe de cierre.
4. **La tasa máxima del generador y del medidor la mide el bloque 1**, que todavía no se ha corrido. Los modos de calibración no se han ejecutado nunca: son código nuevo, probado solo en sus partes puras.
