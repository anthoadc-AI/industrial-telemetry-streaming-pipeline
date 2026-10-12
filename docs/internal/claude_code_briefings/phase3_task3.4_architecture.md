# Task 3.4 — Diagrama de arquitectura y documento de arquitectura

## Contexto
main en 53a53a0, CI verde en los cuatro jobs. Task 3.3 cerrada por Anthony el
2026-10-12. El repo sigue privado y el README es un stub: se escribe en la
Task 3.5, después de esta.

Esta tarea produce `docs/ARCHITECTURE.md`, el primer documento público de
fondo del repo. Lo leerá un entrevistador técnico que no conoce el proyecto.
Debe poder entender en diez minutos qué hace el sistema, cómo fluyen los
datos, qué garantiza y qué no, y por qué se tomó cada decisión.

## Reglas
- Rama `docs/phase3-architecture` desde main. Sin merge hasta la aprobación.
- Camino C: `docs/ARCHITECTURE.md` en inglés. Notas de trabajo y reportes en
  `docs/internal/`, en español.
- La narrativa coincide con el código. Cada afirmación del documento sobre el
  comportamiento del sistema debe poder señalarse en el repo (archivo y
  línea, config o test). Lo que no se pueda señalar no se escribe.
- Ninguna cifra de rendimiento en este documento. Las cifras medidas van al
  README en la Task 3.5 y salen del informe de cierre de la Task 3.3. Aquí
  solo aparecen valores de configuración, leídos de `config/` y del compose.
- Fuente única de verdad: nombres de topics, umbrales, límites y versiones se
  citan desde `config/` y `docker-compose.yml`, no de memoria.
- Los datos son sintéticos y el documento lo dice de forma visible.
- Sin elementos decorativos: cero emojis, cero lenguaje de relleno, cero
  adjetivos de venta. Diagramas con la información mínima necesaria.
- No se cambia código del producto. Si al documentar aparece una
  contradicción entre lo que el sistema hace y lo que debería hacer, se
  reporta como hallazgo; no se arregla ni se maquilla.
- Las limitaciones conocidas se escriben con la misma claridad que las
  capacidades.

## STEP 0 — Diagnóstico y propuesta (sin escribir el documento)
1. Estado: main en 53a53a0, árbol limpio, rama creada.
2. Inventario de lo que el documento debe describir, leído del código:
   componentes, versiones fijadas, perfiles del compose, topics (clave,
   particiones, retención, compactación), esquemas Avro y sus versiones,
   lógica de alarmas, sinks, logging, métricas de latencia, monitor de
   descartes, CI.
3. Material ya redactado que debe incorporarse: el texto de garantías de
   entrega aprobado en el DETENTE 3.3.h (en `docs/internal/phase3_backlog.md`)
   y lo que el backlog marca como contenido obligatorio de la Task 3.4.
4. Propuesta de estructura de `docs/ARCHITECTURE.md`, sección por sección,
   con una línea sobre qué responde cada una. Como mínimo debe cubrir:
   - alcance y qué es sintético;
   - componentes y dónde vive cada uno en el repo;
   - flujo de datos de extremo a extremo;
   - topics y esquemas;
   - lógica de alarmas (umbral, persistencia, histéresis, reconstrucción de
     estado);
   - garantías de entrega, tramo por tramo;
   - modos de fallo: qué pasa cuando cada componente está lento o caído, y
     cómo se detecta;
   - configuración y perfiles de despliegue;
   - observabilidad;
   - decisiones de diseño con su razón y lo que se descartó;
   - limitaciones conocidas, con referencia al backlog;
   - Firebase como sink opcional heredado del curso.
5. Propuesta de diagramas: cuáles (flujo de datos como mínimo; valora uno de
   secuencia para la entrega y los acks, y uno de estados para las alarmas),
   en qué formato y por qué. Evalúa Mermaid embebido en el Markdown (se
   renderiza en GitHub y se versiona como texto) frente a un SVG exportado.
   Indica cómo se comprobará que cada diagrama se renderiza bien en GitHub.
6. Propuesta de comprobación automática: si conviene un test que verifique
   que los nombres y valores citados en el documento (topics, límites,
   versiones de imagen) coinciden con `config/` y el compose, para que el
   documento no se desincronice. Costo y alcance.
7. Lista de contradicciones o huecos encontrados al leer el código para
   documentarlo.

### DETENTE 3.4.a
Detente. Reporta la estructura propuesta, los diagramas propuestos (con un
borrador en texto del diagrama principal), la decisión sobre el test de
sincronía, las contradicciones encontradas y las decisiones que necesita
Anthony. No escribas el documento todavía.

## STEP 1 — Redacción
Solo tras la aprobación del DETENTE 3.4.a, y según lo aprobado.
1. Escribe `docs/ARCHITECTURE.md`.
2. Tabla de trazabilidad en `docs/internal/` (español): por cada afirmación
   de comportamiento del documento, la evidencia en el repo (archivo y línea,
   config o test).
3. Si se aprobó, el test de sincronía, con su job de CI.
4. Verifica el renderizado de los diagramas en GitHub sobre la rama subida.
5. Tests unitarios e integración core, push, CI verde.

### DETENTE 3.4.b
Detente. Reporta: el documento completo, la tabla de trazabilidad, capturas
o confirmación del renderizado de cada diagrama en GitHub, resultado de CI,
desvíos y lo que quedó sin verificar. No hagas merge. El cierre lo aprueba
Anthony.
