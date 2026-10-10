# Task 3.3 — Plan de corridas del STEP 2 (para el DETENTE 3.3.d)

Generado a partir de `config/benchmark.yaml` con `make bench-plan` (`bench/analysis.py: plan`). Ninguna corrida de este plan se ha lanzado. Los tiempos son estimaciones basadas en la corrida de prueba del STEP 1, que tardó 133 s en total para 60 s de envío.

## Condiciones de cada corrida

- **Entorno:** corriente alterna, ningún contenedor de otro proyecto y al menos 1,5 GB libres en el host. El arnés lo comprueba antes de empezar y cada 5 s durante la corrida, e invalida la corrida si algo falla.
- **Stack:** se recrea en cada corrida. El pipeline corre en contenedores, con las vCPU fijadas por servicio (`cpu.cpusets`, D-E): la VM tiene 8 CPU.
- **Duración:** 30 s de calentamiento, 300 s medidos, drenaje hasta lag 0 (como mucho 180 s) y **120 s de enfriamiento** entre corridas (riesgo térmico, DETENTE 3.3.b).
- **CPU:** se registra el uso por contenedor en cada corrida (`cpu.csv`). Un cliente por encima del 80 % de un núcleo invalida la corrida por estar limitada por CPU.
- **Generador:** si su retraso supera p99 5 ms o máximo 100 ms, la corrida es inválida. En la escalera, ese escalón se informa como "limitado por el generador".

## Corridas

| Bloque | Corridas | Activos | Log del bridge | Cifras |
|---|---|---|---|---|
| Latencia a 5 msg/s (la tasa en vivo) | 5 repeticiones | los 5 reales a 1 Hz | INFO | F1 (y sus dos partes), F2, F4 |
| Latencia a 50 msg/s | 5 repeticiones | los 5 reales a 10 Hz | INFO | F1, F2, F4 |
| Escalera de throughput | 1 por escalón: 250, 500, 1000, 2000, 3000, 4000, 6000 y 8000 msg/s | 100 clones (20 por activo real, D-A) | WARNING | F3 (D8) |
| Repeticiones E2 (condicionales) | 4 más en el escalón candidato y 4 en el siguiente | 100 clones | WARNING | confirmación de F3 |

- **Duración estimada por corrida:** 540 s (60 s de montaje, 30 de calentamiento, 300 medidos, 15 de drenaje, 15 de bajada y 120 de enfriamiento).
- **Total:** 18 corridas fijas, unas **2,7 h** de portátil enchufado. Con las 8 repeticiones condicionales, **3,9 h** en el peor caso.
- **Sesiones:** se puede repartir. Cada corrida es independiente y escribe su propio directorio en `bench/results/`. Para no mezclar condiciones térmicas, propongo que cada bloque se haga entero en una misma sesión.

## Lo que no cubre este plan (desvíos de la implementación)

1. **F5, F6 y F7 (camino a ThingsBoard) no tienen todavía modo en el arnés.** El arnés mide el pipeline (perfil core). El sink necesita un modo con el perfil full, el sink en contenedor, el observador WebSocket en contenedor (la prueba de concepto de 1.1.2 está en `scripts/bench/diagnostics/`) y una parada controlada de ThingsBoard para F7. No está construido. Decisión pendiente: añadirlo antes del STEP 2 o publicar solo F1–F4.
2. **El generador programa sobre el reloj de pared de la VM, no sobre el monotónico.** En la VM, `CLOCK_MONOTONIC` y `CLOCK_REALTIME` se separan unos 100 ppm (−94 ppm medidos en la prueba), y con una programación monotónica los mensajes salían cada vez antes que su `ts`. Todos los timestamps de la corrida usan el reloj de pared, y un salto de ese reloj aparece como retraso del generador e invalida la corrida.
3. **La pendiente del lag admite 0,001 s/s, no 0.** Con 0 exacto, el ruido de un lag estable fallaría la mitad de las corridas. El valor está en la config y el informe permite recalcular con otro.
4. **D8 (sostenible o no) solo se evalúa en las corridas de throughput.** A tasas de latencia, un solo mensaje es una fracción grande de un segundo de lag, y la pendiente sale como ruido de cuantización. Las corridas de latencia informan solo de su validez.
5. **La tasa máxima del generador no está medida.** Es un solo proceso de Python. Si no llega a 6000–8000 msg/s, la escalera se informará como limitada por el generador. Repartirlo en varios procesos queda como opción, no implementada.
6. **Las funciones de E/S del arnés, el generador y el medidor llevan `pragma: no cover`**, y no solo `main()` como es costumbre en el repo. Las ejercita la corrida de prueba. Su lógica pura está en módulos aparte con tests (`bench/schedule.py`, `criteria.py`, `analysis.py`, `histogram.py`, `clones.py`, `compose.py` y `report.py`, todos entre el 98 y el 100 %).
