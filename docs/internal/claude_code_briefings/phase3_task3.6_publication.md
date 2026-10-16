# Task 3.6 — Revisión final y publicación del repositorio

## Contexto
main en 0604a08, CI verde en los cuatro jobs. Tasks 3.1 a 3.5 cerradas,
README y caso de estudio en main. El repo sigue privado. Esta tarea lo deja
listo para hacerse público y cierra la Fase 3. El cambio a público lo hace
Anthony a mano en GitHub; Claude Code no lo hace.

Lo que un visitante verá primero: el README, la lista de archivos de la
raíz, el historial de commits y los runs de CI. Todo eso se revisa como lo
haría un desconocido con criterio.

## Reglas
- Rama `chore/phase3-publication` desde main. Sin merge hasta la aprobación.
- Solo lectura en el STEP 0. Ningún cambio al producto; los arreglos del
  STEP 1 son de documentación, nombres, metadatos y limpieza.
- Nada del historial de git se reescribe. Si se encuentra un secreto en un
  commit antiguo, se reporta y se detiene: la decisión (rotar la credencial
  o reescribir) es de Anthony.
- Camino C: todo lo público en inglés.

## STEP 0 — Auditoría (sin cambios)
1. Estado: main en 0604a08, árbol limpio, rama creada.
2. **Secretos en todo el historial.** Ejecuta `check_secrets.py` sobre cada
   commit (o `git log -p` filtrado) y, además, busca a mano patrones que el
   script no cubra: tokens, URLs con credenciales, el identificador del
   proyecto de Firebase, rutas de usuario de Windows, direcciones de correo
   personales, nombres de máquina. Revisa también `bench/results/` y
   `docs/internal/`, que son públicos aunque sean internos.
3. **Lectura como visitante.** Lista de la raíz: cada archivo o carpeta debe
   tener razón de estar. Enlaces internos y externos de README, CASE_STUDY
   y ARCHITECTURE: todos deben resolver. Archivos vacíos, de prueba o
   huérfanos. Nombres de archivo inconsistentes. Comentarios en español
   fuera de `docs/internal/`. Referencias a rutas locales.
4. **Metadatos del repo.** LICENSE presente y coherente con el README;
   `.env.example` completo y sin valores reales; `.gitignore` y
   `.gitattributes` coherentes; `package.json` y `requirements.txt` sin
   dependencias sin uso (solo reporta).
5. **`make install` en un clon limpio.** Clona el repo en una carpeta
   temporal fuera del árbol de trabajo y ejecuta `make install` y
   `make test`. Es lo primero que hará un visitante y nunca se ha probado
   desde cero. Registra tiempos y cualquier paso que falte.
6. **CI visible.** Confirma que el flujo de trabajo y la insignia del README
   funcionarán en un repo público (la insignia hoy no se puede comprobar
   por ser privado). Revisa que ningún job dependa de secretos del repo que
   un fork no tendría.
7. **Tamaño.** Tamaño del clon y de los mayores archivos versionados.
8. **Documentos internos.** `docs/internal/` es público. Revisa que ninguna
   nota contenga datos personales, rutas locales o lenguaje que no quieras
   que lea un reclutador; reporta las frases concretas, no las cambies.
9. **Descripción y temas del repo.** Propón la descripción de una línea y
   los temas (topics) de GitHub, en inglés.

### DETENTE 3.6.a
Detente. Reporta cada hallazgo con archivo y línea, clasificado en: bloquea
la publicación / conviene arreglar / se deja como está. Propón la
descripción y los temas. No cambies nada.

## STEP 1 — Arreglos y cierre
Solo tras la aprobación del DETENTE 3.6.a.
1. Aplica los arreglos aprobados, en commits pequeños por tipo.
2. Informe de cierre de la Fase 3 en `docs/internal/`, en español e inglés,
   con la misma estructura que los anteriores: qué se entregó por tarea,
   hallazgos en Problema-Acción-Resultado, cifras finales con su fuente,
   desvíos, lo que queda en el backlog.
3. Unit tests, push, CI verde.

### DETENTE 3.6.b
Detente. Reporta los cambios, el informe de cierre y CI. Sin merge, sin tag.

## Después de la aprobación de Anthony (en un mensaje aparte)
1. Merge `--no-ff` a main, push, CI verde en main.
2. Tag anotado `v3-quantified-evidence` sobre el merge, con un mensaje que
   resuma la Fase 3. Push del tag.
3. Anthony cambia el repo a público en GitHub, añade la descripción y los
   temas, y comprueba la insignia de CI en el README.
