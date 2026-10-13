# Task 3.5, STEP 0: inventario y esqueletos (DETENTE 3.5.a)

Rama `docs/phase3-readme`, desde main `c576f2b`. Briefing: `docs/internal/claude_code_briefings/phase3_task3.5_readme.md`. Aprobado por Anthony el 2026-10-13, con las decisiones de la sección 7.

## 1. Estado

- Rama en `604f2a6`, un commit por delante de main `c576f2b` (igual a `origin/main`), árbol limpio, rama solo local.
- `604f2a6` contiene las tres capturas, el briefing y la regla `!docs/images/*.png` al final de `.gitignore`; `git check-ignore` confirma que las imágenes no se ignoran.
- Capturas:
  - `docs/images/01_plant_fault_active.png`: 154.953 B, 1917 × 1078 px;
  - `docs/images/02_pump01_vibration_step.png`: 306.683 B, 1917 × 1078 px;
  - `docs/images/03_plant_alarms_cleared.png`: 114.277 B, 1917 × 1075 px.
- Ningún commit toca `processing/thingsboard/` desde el 2026-10-04: las capturas muestran el dashboard actual.
- `make bench-report` tarda 3 s y reproduce literalmente las 24 filas comprobadas de la sección 1 del informe de cierre de la Task 3.3. Da la misma salida desde un `git archive` limpio de `bench/results/`: no necesita los `raw/` ignorados.

## 2. Inventario de cifras

"Informe" es `docs/internal/phase3_task3.3_closure_report.en.md`; su sección 1 es copia de `make bench-report`. Milisegundos enteros, mediana de 5 corridas y rango entre ellas.

| Cifra | Valor | Fuente |
|---|---|---|
| F1 e2e a 50 msg/s, p50 / p95 / p99 | 7 (7-8) / 11 (11-12) / 13 (12-13), n = 15000 por corrida | informe:26 |
| F2 alarma a 50 msg/s, p50 / p95 | 35 (34-36) / 55 (52-56), n = 300; sin p99 | informe:29 |
| F2 a 5 msg/s | 34 (34-39) / 55 (48-58) | informe:24 |
| F4 produce-to-ack a 50 msg/s | 6 / 8 / 8 | informe:30 |
| F3 | "Candidate: 2000/s; runs 5 of the 5 E2 needs; confirmed."; 3000/s no medible válidamente (generador al límite) | informe:39-45, 54 |
| F1 a 2000 msg/s | 6 (6-6) / 11 (10-16) / 26 (19-59), n = 600000 | informe:31 |
| F2 a 2000 msg/s | 28 / 46, con 100 activos clonados | informe:34, 153 |
| Condiciones | un portátil, WSL2, 8 vCPU fijadas por servicio, un host; ventanas de 300 s; Kafka en tmpfs, un broker, RF 1; ~218 B sintéticos; F2 termina en el ack de Kafka; F5-F7 no publicadas | informe:151-160 |
| Umbrales de vibración | 4,5 y 7,1 mm/s, coinciden con ISO 20816-3 (límites B/C y C/D, Grupo 2, soportes flexibles) | ARCHITECTURE.md:234 |
| Camino a ThingsBoard | 10 msg/s por activo, por configuración | ARCHITECTURE.md:437, 448 |
| pump-01 con imbalance | ~6,5 mm/s: WARNING, no CRITICAL | phase2_closure_report.md:210 |
| Último run de main | 38472910691 sobre `c576f2b`: 447 Python, 119 Node; core 16 passed, 16 skipped; full 32 passed; Python 98,54 % total y 96,68 % ramas (845/874); Node líneas 100 % (1159/1159), ramas 98,7 % (305/309) | log de CI |

## 3. Hallazgos candidatos

Los cinco del README (Problema-Acción-Resultado):
1. Pérdidas en la cola de Mosquitto: límite medido 1040 mensajes por cliente; 61.140 descartados en el diagnóstico a 3000 msg/s; cola 60.000 / 25 MiB y monitor; test de pausa de 1040/5000 a 5000/5000 (informe:105-119).
2. Healthcheck con JVM: broker en reposo de 18,0 % a 5,9 % de un núcleo; muestras ≥ 50 %: 17 a 1 (informe:121-127).
3. StopTimeout de Docker Desktop: 1 s frente a 10 s del motor; el consumer necesitaba 1,5-3,6 s; `stop_grace_period`; salidas 0 o 143, ninguna por SIGKILL (informe:82-97).
4. CLEARED espurio tras un restore (ARCHITECTURE.md:282-284; `phase3_fix_h3_result.md`).
5. Alarma fantasma tras un reproceso (ARCHITECTURE.md:286-288).

Otros, fuera del README: artefacto de latencia host-VM (informe:77-80); cola SequentialByOriginator de ThingsBoard, 43 de 48 alarmas (phase3_backlog.md:112); particionador por defecto, 19 de 48 alarmas tras un reproceso (phase3_backlog.md:12); coste del log por mensaje (informe:129-135); defectos de la Fase 2 (phase2_closure_report.md:268, 274).

## 4. Esqueleto del README

1. Qué es y aviso de datos sintéticos. 2. Arquitectura: diagrama de ARCHITECTURE.md §2 literal, cinco líneas, enlace. 3. Cifras medidas: condiciones primero, tres filas. 4. Los cinco hallazgos. 5. Cómo correrlo. 6. Capturas con pie. 7. Lógica de alarmas en tres líneas, ISO 20816-3. 8. Verificación: CI, tests, cobertura. 9. Limitaciones, enlace a §13. 10. Origen: curso y `v0-coursework`. 11. Why this matters, enlace al caso de estudio.

## 5. Esqueleto de `docs/CASE_STUDY.md` (~1500 palabras)

1. Problema de negocio, cualitativo (200). 2. Streaming frente a lotes (200). 3. Qué se construyó y qué decisiones responden a problemas de planta: orden por activo, persistencia e histéresis, entrega sin pérdida, reconciliación, defectos de reproceso (400). 4. Qué demostró el benchmark y qué no (200). 5. Aplicación a una planta real: qué se reemplaza, qué se mantiene, qué falta (300). 6. Qué aporta la ingeniería de datos, con ejemplos del repo (250).

## 6. Comprobación automática propuesta

`tests/unit/test_public_docs.py`, en `make test` (job `checks`): las cifras de la tabla del README igual a la salida de `bench.report.render()` sobre `bench/results/`; la frase de F3 igual a la línea "Candidate"; todo enlace relativo e imagen del README y del caso de estudio existe, y las anclas a `ARCHITECTURE.md` existen; las rutas entre comillas invertidas existen; el bloque mermaid del README es igual al de ARCHITECTURE.md §2. Las cifras de CI no se comprueban (no se calculan sin conexión): van en la tabla de trazabilidad.

## 7. Decisiones de Anthony (DETENTE 3.5.a, 2026-10-13)

1. `make up` y `make up-pipeline` como alternativas, con una línea sobre cuándo usar cada una.
2. Un comando del simulador con el mismo fallo de las capturas (`pump-01:imbalance`), variantes en el host y con el perfil de compose; verificado en STEP 1 con el dashboard abierto: el operador ve lo que muestra la captura 01.
3. Una insignia del workflow de CI (estado en vivo) y la línea que cita el run 38472910691 sobre `c576f2b` con su fecha. Ninguna otra insignia.
4. Cobertura: las dos cifras impresas, etiquetadas como las imprime coverage.py.
5. Tabla de cifras: F1 a 50 msg/s, F2 a 50 msg/s, F3 a 2000 msg/s con F1 a ese ritmo; mediana con rango entre corchetes, tal como se imprimen.
6. F2 se llama "sample to the alarm event acknowledged by Kafka".
7. StopTimeout: estados de salida, sin tiempos de `make down`.
8. Exactamente cinco hallazgos en el README; el particionador y la cola de ThingsBoard van al caso de estudio.
9. Alturas de ventana: diferencia despreciable; se cierra el punto del backlog.
10. Sin cifras económicas ni de mercado. ISO 20816-3 nombrada con enlace marcado como externo si existe una página oficial estable; si no, solo el nombre.
11. Voz impersonal en los dos documentos.
12. Este informe, en `docs/internal/`, en español.
13. Push de la rama ahora y tras cada commit del STEP 1.
