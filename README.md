# Trabajo Práctico - Coordinación

## Autores

| Nombre | Apellido      | Mail           | Padrón |
|--------|---------------|----------------|--------|
| Ian    | von der Heyde | ivon@fi.uba.ar | 107638 |

---

## Diagrama de arquitectura
![ ](./imgs/diagrama_final.jpg "Arquitectura del sistema implementado")

*Fig. 1: Arquitectura del sistema implementado con coordinación por anillo*

---

## Índice

1. [Supuestos](#1-supuestos)
2. [Protocolo interno de mensajes](#2-protocolo-interno-de-mensajes)
3. [Middleware](#3-middleware)
4. [Gateway y `message_handler`](#4-gateway-y-message_handler)
5. [Sum](#5-sum)
6. [Aggregation](#6-aggregation)
7. [Join](#7-join)
8. [Escalabilidad](#8-escalabilidad)
9. [Alternativas de coordinación consideradas](#9-alternativas-de-coordinación-consideradas)

---

## Visión general de la solución

Para resolver los tres problemas centrales del TP:

- **Múltiples clientes concurrentes**: cada mensaje lleva un `client_id` único generado por el gateway. Todos los componentes mantienen estado separado por cliente y nunca mezclan sesiones.

- **Coordinación de EOF entre réplicas de Sum**: el gateway cuenta exactamente cuántos mensajes `data` envió (`total_messages`) e incluye ese número en el `eof`. Las réplicas de Sum usan un **anillo de conteo**: la que recibe el `eof` circula un token sumando los counts de cada nodo; cuando la suma iguala `total_messages`, todas flushean. Si no alcanza, se reintenta.

- **Distribución de carga hacia Aggregation**: Sum hace sharding determinístico por fruta (`zlib.crc32 % AGGREGATION_AMOUNT`), de modo que cada fruta siempre va al mismo Aggregator. Esto evita broadcast y procesamiento redundante.


---

## 1. Supuestos

El diseño parte de los siguientes supuestos:

1. **Sin caída de instancias en ejecución**: ningún proceso muere mientras el sistema está procesando datos. El manejo de SIGTERM cubre el apagado ordenado, no la tolerancia a fallos en tiempo de ejecución.
2. **Comunicación estable durante la ejecución**: una vez iniciado el sistema, la conexión con RabbitMQ no se interrumpe. Si esto ocurriera, el proceso libera recursos y termina; no intenta recuperarse.
3. **FIFO dentro de cada cola**: los mensajes en una misma cola se entregan en el orden en que fueron publicados.
4. **Fairness del scheduler de RabbitMQ**: cuando hay mensajes en más de una cola, eventualmente todos son procesados.
5. **Sin reentregas en ejecución nominal**: RabbitMQ no reentrega mensajes ya procesados en condiciones normales.

Bajo estos supuestos no hay doble conteo, no hay flushes incompletos y no se necesitan mecanismos de deduplicación.

**Consecuencias de los supuestos sobre limpieza de estado**: si un cliente se desconectara a mitad de sesión (sin enviar EOF), la entrada en `sessions` quedaría huérfana indefinidamente en todos los componentes. No se implementó un mecanismo de TTL ni de limpieza de clientes muertos porque el supuesto es que eso no ocurre. En un sistema sin ese supuesto sería necesario agregar un heartbeat del cliente o un timeout por inactividad para liberar la memoria asociada a sesiones abandonadas.

---

## 2. Protocolo interno de mensajes

Todos los mensajes internos son JSON codificado en UTF-8. Cada mensaje lleva obligatoriamente los campos `kind` (identifica el tipo, definido en `message_protocol.internal.Kind`) y `client_id` (permite que múltiples clientes coexistan sin mezclar estado).

Los tipos de mensajes y sus campos adicionales son:

| `kind` | Emisor | Cola destino | Receptor | Campos adicionales |
|---|---|---|---|---|
| `data` | Gateway | `INPUT_QUEUE` | Sum | `fruit: str`, `amount: int` |
| `eof` | Gateway / Sum (retry) | `INPUT_QUEUE` | Sum | `total_messages: int` |
| `ring_token` | Sum (líder o no-líder) | `{SUM_PREFIX}_ring_{i+1}` | Sum siguiente | `accumulated_count: int` |
| `ring_finish` | Sum (líder lo inicia; cada no-líder lo reenvía) | `{SUM_PREFIX}_ring_{i+1}` | Sum siguiente | — |
| `sum_partial` | Sum | `{AGGREGATION_PREFIX}_{i}` | Aggregation_i | `fruit: str`, `amount: int` |
| `sum_done` | Sum | `{AGGREGATION_PREFIX}_{i}` (todos) | Todas las Aggregation | `src_id: int` |
| `agg_top` | Aggregation | `join_queue` | Join | `src_id: int`, `top: [[str, int]]` |
| `final_top` | Join | `RESULTS_QUEUE` | Gateway | `top: [[str, int]]` |

**Notas de diseño:**
- `eof` puede ser re-encolado por el propio Sum líder cuando el anillo detecta que no todos los datos fueron procesados (ver Sección 5). En ese caso el emisor es Sum, no Gateway, pero el campo `total_messages` es el mismo valor original.
- `sum_partial` va a una sola Aggregation (la que corresponde por sharding); `sum_done` va a **todas**.
- `ring_finish` recorre todo el anillo: el líder lo inicia, cada no-líder hace flush y lo reenvía al siguiente, y el líder (que lo recibe último) hace flush pero no lo reenvía. No lleva campos adicionales; el `client_id` es suficiente para identificar la sesión.

---

## 3. Middleware

Implementé el middleware sobre `pika.BlockingConnection` (RabbitMQ). La clase `MessageMiddlewareQueueRabbitMQ` encapsula una conexión, un channel y el nombre de una cola; `MessageMiddlewareExchangeRabbitMQ` hace lo mismo para un Direct Exchange.

### Conexión con reintentos

Al arrancar, RabbitMQ puede no estar disponible todavía (el contenedor está levantando). En lugar de fallar inmediatamente, cada componente reintenta la conexión con backoff exponencial: el delay entre intentos empieza en `_RETRY_BASE_DELAY` segundos y se duplica hasta un máximo de `_RETRY_MAX_DELAY`, por hasta `MAX_ATTEMPTS` intentos (configurable por variable de entorno). Si se supera ese límite, el proceso lanza `MessageMiddlewareDisconnectedError` y termina.

Elegí backoff exponencial sobre reintentos a intervalo fijo porque reduce la carga sobre RabbitMQ en arranques concurrentes donde muchos contenedores intentan conectarse al mismo tiempo.

### Durabilidad

Todas las colas se declaran con `durable=True` y los mensajes se publican con `delivery_mode=2`. Esto garantiza que si RabbitMQ se reinicia, las colas y sus mensajes sobreviven. Bajo los supuestos del TP esto no debería ocurrir, pero lo mantuve porque no agrega complejidad de código ya que fue realizado para el TP anterior, aunque reconozco que no se aprovecha de la mejor forma en esta implementación.

No se usan **quorum queues**: son colas replicadas vía Raft que requieren un cluster de al menos 3 nodos de RabbitMQ para dar garantías reales. El sistema corre con un único nodo de RabbitMQ, por lo que en un nodo solo añadirían overhead de consenso sin ningún beneficio de replicación.

### Manejo de errores en ejecución

Si ocurre cualquier error AMQP durante el consumo o la publicación, el proceso lanza `MessageMiddlewareDisconnectedError` o `MessageMiddlewareMessageError`, libera recursos y termina. No hay recuperación algorítmica, ya que por nuestros supuestos esto no debería ocurrir en condiciones normales.

---

## 4. Gateway y `message_handler`


### Multi-cliente

Para soportar múltiples clientes concurrentes, cada instancia de `MessageHandler` genera un `_client_id` único al crearse (`uuid4().hex`). Todos los mensajes que serializa llevan ese identificador.

- `serialize_data_message`: serializa un par (fruta, cantidad) como mensaje `data` con `client_id` y un campo `kind`. Incrementa `_data_count` en cada llamada.
- `serialize_eof_message`: serializa el fin de ingesta como mensaje `eof` con `client_id` y `total_messages = _data_count`. Este conteo exacto es la base del protocolo de coordinación de Sum.
- `deserialize_result_message`: filtra mensajes de resultado por `client_id`. Retorna `None` si el mensaje no pertenece a este cliente. El gateway itera sus handlers en orden y el primero que retorna algo distinto de `None` se queda con el mensaje.

---

## 5. Sum

Sum es el componente más complejo porque es el único que necesita coordinación distribuida: varias réplicas comparten una work queue, pero solo una recibe el EOF de cada cliente.

### Estado por sesión

Mantuve un diccionario `sessions` indexado por `client_id`. Cada entrada tiene:
- `count`: cantidad de mensajes `data` procesados por esta instancia de Sum para ese cliente.
- `partial_by_fruit`: acumulado parcial por fruta.
- `is_leader`: `True` si esta instancia consumió el `eof` de este cliente.
- `total_messages`: el valor de `total_messages` del `eof` (solo válido mientras `is_leader` es `True`).

### Protocolo de coordinación por anillo

El problema central es: ¿cómo saben todas las instancias que ya procesaron todos los datos de un cliente, si el EOF lo recibe solo una? Mi solución es un **anillo de conteo**:

1. La instancia que consume el `eof` de un cliente se convierte en **líder** para ese cliente y envía un token `ring_token` con `accumulated_count` igual a su propio `count` hacia el siguiente nodo del anillo (`next_ring_queue` = `{SUM_PREFIX}_ring_{(ID+1) % SUM_AMOUNT}`).
2. Cada instancia no-líder suma su propio `count` al token y lo reenvía.
3. Cuando el token regresa al líder, `accumulated_count` ya contiene la suma de todos los counts (incluyendo el propio, que fue el valor inicial del token). Si ese total iguala `total_messages`, todos los datos fueron procesados: el líder envía `ring_finish` por el anillo para que las demás instancias de Sum flusheen también.
4. Si el total es menor, significa que algunos datos aún no fueron procesados en el momento en que el token circuló. El líder resetea su estado (`is_leader = False`, `total_messages = None`) y re-encola el `eof` en `INPUT_QUEUE` para iniciar una nueva vuelta más adelante.
5. Si el total es mayor a `total_messages`, hay una violación de invariante: bajo los supuestos del sistema (sin reentregas, comunicación estable) esto no debería ocurrir nunca. Si ocurriera indicaría un bug en el counting o una reentrega inesperada del broker. En ese caso el sistema logguea el error y lanza una excepción para terminar limpiamente.

**Caso borde: Sum con count=0.** Si una instancia de Sum no procesó ningún dato de un cliente (porque todos los mensajes fueron al resto), su `count` es 0. Al pasar el token, suma 0 al `accumulated_count` y lo reenvía. El protocolo funciona exactamente igual — 0 es un aporte válido.

El `ring_finish` se propaga por el anillo: cada nodo hace **flush** (envía sus `sum_partial` y `sum_done`) y reenvía el `ring_finish`, excepto el líder que hace flush pero no reenvía.

### Colas del ring

Cada Sum_i tiene:
- `_ring_inbox_name` = `{SUM_PREFIX}_ring_{ID}`: su buzón de entrada; Sum_{i-1} le escribe aquí.
- `next_ring_queue` = `{SUM_PREFIX}_ring_{(ID+1) % SUM_AMOUNT}`: el buzón del siguiente; Sum_i escribe aquí.

**Pre-declaración al arranque**: al inicializarse, cada Sum declara las `SUM_AMOUNT` colas del ring (no solo la propia) via `input_queue.declare_queue(...)`, reutilizando el channel ya abierto de INPUT_QUEUE. De este modo no se abren conexiones adicionales solo para declarar: la única conexión extra del ring es `next_ring_queue` (para publicar al nodo siguiente). Esto garantiza que si Sum_i envía un `ring_token` antes de que Sum_{i+1} haya arrancado, la cola ya existe en RabbitMQ y el mensaje no se pierde.

### Consumo de dos colas en un solo thread

Sum_i necesita consumir tanto de `INPUT_QUEUE` como de `_ring_inbox_name` simultáneamente. `pika.BlockingConnection` tiene un único event loop y solo admite una llamada a `start_consuming()` por conexión. La solución es registrar ambos consumers en el **mismo channel** antes de arrancar el loop, a través del método `add_queue_consumer` del middleware.

Esto significa que el objeto `input_queue` (de `INPUT_QUEUE`) también es el que registra el consumer del ring. Elegí no usar threads ni multiprocessing porque requeriría locks sobre el diccionario `sessions`, añadiendo complejidad desproporcionada. Esto se justifica por el volumen de mensajes: el ring genera O(`SUM_AMOUNT`) mensajes por vuelta por cliente, órdenes de magnitud menor que los mensajes de datos; no hay ganancia grande de paralelismo al procesarlos en paralelo e introducía complejidad adicional.

Si `add_queue_consumer` falla (por ejemplo, por un error AMQP al declarar la cola), la excepción se propaga hacia afuera del bloque `try/finally` de `start()`, lo que garantiza que el `finally` se ejecuta y todas las conexiones se cierran.

### Prefetch y no-starvation del ring

Configuré `prefetch_count=1` **por consumer** (no global). Según la especificación AMQP y la documentación de RabbitMQ, con `global=False` (el default de pika), el límite de prefetch aplica de forma independiente a cada consumer del channel: RabbitMQ puede tener 1 mensaje sin ACK en vuelo para el consumer de INPUT_QUEUE **y** simultáneamente 1 mensaje sin ACK en vuelo para el consumer del ring.

Esto garantiza **no-starvation**: el mensaje del ring está pre-entregado en el buffer del cliente incluso mientras se está procesando un mensaje de INPUT. Cuando el mensaje de INPUT es ACKeado, el evento del ring ya está disponible para procesar en el siguiente ciclo del event loop.

Lo que **no** garantiza es alternancia estricta 1-dato/1-ring. Si INPUT_QUEUE tiene muchos mensajes, el patrón típico es: procesar 1 dato (ACK) → pika procesa el ring pre-entregado (ACK) → RabbitMQ entrega el siguiente de INPUT y el siguiente del ring → repetir. En la práctica el ring se procesa dentro de 1-2 mensajes de datos de haber llegado, lo cual es suficiente para este diseño.

### Sharding hacia Aggregation

Al hacer flush, Sum distribuye los `sum_partial` entre los aggregators usando `zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT`. Elegí `zlib.crc32` porque es determinístico entre corridas de Python (a diferencia de `hash()`), rápido, y distribuye bien strings cortos como nombres de frutas. Cada fruta siempre va al mismo aggregator, lo que evita que cada aggregator tenga que esperar datos de todas las frutas.

El mensaje `sum_done` se envía a **todos** los aggregators, para que cada uno sepa cuántas instancias de Sum terminaron.

### SIGTERM

El handler de SIGTERM activa el flag `_shutdown_requested` y llama a `input_queue.stop_consuming()`. El flag cubre el caso en que la señal llega antes de que empiece el loop: como `stop_consuming()` ahí no hace nada, luego se chequea el flag y se evita entrar a `start_consuming()`.

`start_consuming()` retorna y el bloque `finally` cierra todas las conexiones abiertas:
- `input_queue`: la conexión de INPUT_QUEUE (que también hostea el consumer del ring inbox).
- `aggregator_queues[i]`: una conexión por cada aggregator (usadas para publicar `sum_partial` y `sum_done`).
- `next_ring_queue`: conexión de ring, usada para publicar al nodo siguiente. Las demás ring queues se declararon en el channel de `input_queue` sin abrir conexiones propias.

**Excepción no cubierta**: si una excepción ocurre durante el flush (dentro de `_flush()`), el mensaje `ring_finish` que lo disparó queda sin ACK. RabbitMQ lo reentregará al restartar el consumer, pero bajo el supuesto de que no hay caídas durante la ejecución esto no ocurre. No se implementó un flush idempotente ni deduplicación de `sum_partial` porque excede los supuestos del TP.

---

## 6. Aggregation

Cada instancia de Aggregation consume de su propia cola `{AGGREGATION_PREFIX}_{ID}`, a la que solo Sum le envía datos hasheados para ella.

Mantiene un diccionario `sessions` por `client_id` con:
- `fruits`: acumulado de `sum_partial` por fruta.
- `done_count`: cuántos `sum_done` (de las instancias de Sum) recibió para este cliente.

Cuando `done_count` alcanza `SUM_AMOUNT`, sabe que todas las instancias de Sum hicieron flush para ese cliente. En ese momento calcula el top parcial de sus frutas (usando la comparación de `FruitItem`) y envía un mensaje `agg_top` con `src_id` hacia Join. Luego limpia la sesión.

Decidí que cada Aggregation tenga su propia cola directa (en lugar de un Exchange con fanout) porque Sum ya hace el sharding: sabe exactamente a qué aggregator mandar cada fruta.

### SIGTERM

Igual que en Sum: el handler activa `_shutdown_requested`, llama a `stop_consuming()`, y el flag es verificado antes de entrar al loop para cubrir la race condition. El `finally` cierra `input_queue` y `output_queue`.

---

## 7. Join

Join consume de una única cola (`INPUT_QUEUE`, que en el sistema es `join_queue`) y recibe mensajes `agg_top` de todas las instancias de Aggregation.

Mantiene un diccionario `sessions` por `client_id` con:
- `items`: top parcial acumulado, fusionando los tops de cada Aggregation con `FruitItem`.
- `received`: cuántos `agg_top` recibió para este cliente.

Cuando `received` alcanza `AGGREGATION_AMOUNT`, tiene la información completa. Ordena todos los items usando la comparación de `FruitItem`, extrae el top-N (`TOP_SIZE`) y emite un mensaje `final_top` hacia `RESULTS_QUEUE` (de donde el gateway lo retira y lo entrega al cliente).

Decidí que Join acumule y re-ordene los tops parciales (en lugar de confiar en que ya vienen ordenados) porque cada Aggregation solo ve un subconjunto de las frutas: el top parcial de Aggregation_0 podría tener frutas con conteos mayores que las del top de Aggregation_1, y solo al unirlos se puede determinar el top global correcto.

### SIGTERM

Igual que los anteriores: `_shutdown_requested` + `stop_consuming()` + verificación antes del loop.

---

## 8. Escalabilidad

### Respecto a los clientes

Cada mensaje interno lleva `client_id`. Todas las instancias (Sum, Aggregation, Join) mantienen estado separado por cliente en sus diccionarios `sessions`. Los clientes pueden procesarse de forma completamente concurrente sin interferencia.

El gateway filtra resultados por `client_id` en `deserialize_result_message`, por lo que cada cliente recibe solo su resultado aunque la cola de resultados sea compartida.

### Respecto a la cantidad de controles Sum

Agregar más instancias de Sum distribuye la carga de datos de `INPUT_QUEUE` automáticamente (es una work queue). El protocolo del anillo escala linealmente: cada vuelta cuesta O(`SUM_AMOUNT`) mensajes de coordinación, independientemente del volumen de datos. El número de conexiones abiertas por Sum crece con `SUM_AMOUNT` (una por ring queue + una por aggregator) pero se trata de conexiones de publicación, no de consumo.

### Respecto a la cantidad de controles Aggregation

Agregar más instancias de Aggregation reduce la cantidad de frutas que procesa cada una. Sum hace el sharding automáticamente usando `zlib.crc32 % AGGREGATION_AMOUNT`: al cambiar `AGGREGATION_AMOUNT`, la distribución se ajusta sin modificar código. Join espera `AGGREGATION_AMOUNT` mensajes `agg_top` antes de emitir el resultado final, por lo que escala su barrera de sincronización automáticamente.

---

## 9. Alternativas de coordinación consideradas

### 1. Reencolado de SUM_AMOUNT copias del EOF

La idea era reencolar tantas copias del `eof` como instancias de Sum hay en `INPUT_QUEUE`, apostando a que el round-robin de RabbitMQ distribuya exactamente una copia a cada instancia.

**Problema fundamental**: RabbitMQ no garantiza round-robin estricto. El reparto depende del prefetch, la velocidad de ACK y el estado interno del broker. Con carga despareja, una instancia podría recibir dos copias y otra ninguna. El protocolo no tendría forma de detectarlo: dos flushes desde la misma instancia generarían doble conteo en Aggregation, y la instancia que no recibió el EOF nunca flushearía. El esquema falla de forma silenciosa y no determinística.


### 2. Canal compartido con `global=True` + fanout de EOF

Cada Sum consume de dos colas en el mismo canal: `INPUT_QUEUE` (datos y `eof_request`) y una cola personal durable bindeada a un fanout exchange de control (`{SUM_PREFIX}_eof_{ID}`). El canal se configura con `basic_qos(prefetch_count=1, global_qos=True)`, que da **1 crédito total al canal** en lugar de 1 por consumer (por instancia de Sum).

La garantía clave es causal: el gateway publica datos y luego `eof_request` en orden FIFO. La Sum que desencola `eof_request` sabe que todos los datos ya salieron de `INPUT_QUEUE` — están siendo procesados por alguna réplica. En ese momento publica `eof_control` al fanout exchange, que lo entrega simultáneamente a las `N` colas personales. Gracias a `global_qos=True`, ninguna réplica puede recibir `eof_control` mientras tiene un mensaje de datos sin ACKear: el crédito del canal está ocupado. Cuando llega `eof_control`, el estado parcial de esa réplica está completo y puede flushear directamente — **sin conteo distribuido**.

**Por qué no la usé**: el protocolo es correcto y no compromete el throughput — cada Sum tiene su propia conexión y canal, por lo que `global_qos=True` serializa únicamente la alternancia entre los dos consumers *dentro de esa instancia*, no entre instancias. Las N réplicas siguen procesando en paralelo. La razón por la que no la elegí es que se apoya casi completamente en garantías del protocolo AMQP (`global_qos` + FIFO de cola) en lugar de coordinación explícita a nivel de aplicación, que es lo que el enunciado pide diseñar e implementar.

### 3. Broadcast all-to-all de counts en lugar de anillo

La variante mantiene el mismo mecanismo de conteo que la solución elegida, pero reemplaza el anillo por dos rondas de broadcast:

1. La instancia que recibe el `eof` publica un mensaje de control con `total_messages` a **todas** las instancias (incluyéndose) vía fanout exchange.
2. Cada instancia que recibe ese mensaje publica su propio `count` a todas las demás (también vía fanout o direct con N routing keys).
3. Cada instancia acumula los N counts recibidos, suma, y compara contra `total_messages`. Si es igual, flushea; si es menor, el líder reencola el `eof`; si es mayor, error.

**Por qué no la usé**: la lógica de decisión es equivalente al anillo — ambas son snapshots del conteo total en un instante dado, con el mismo mecanismo de retry. La diferencia está en el volumen de mensajes de coordinación: el anillo genera O(N) mensajes por vuelta (token + finish), este esquema genera O(N²) (cada una de las N instancias difunde su count a las otras N). Además requiere que cada instancia abra N conexiones de salida para el broadcast de counts, frente a la única conexión de ring del anillo. Para N pequeño la diferencia es irrelevante, pero el anillo es estrictamente más barato en mensajes y conexiones sin perder expresividad.




### 4. Reencolado del EOF con conjunto de instancias visitadas

La idea era incluir en el `eof` un conjunto de IDs de instancias que ya lo procesaron. Cada instancia que lo recibe agrega su propio ID y lo reencola en `INPUT_QUEUE`, a menos que el conjunto ya contenga todos los IDs. La instancia que cierra el conjunto sabe que es la última y no reencola.

**Por qué no la usé**: `INPUT_QUEUE` es una work queue competitiva — RabbitMQ entrega el mensaje al primer consumer listo, sin garantía de equidad entre instancias. Una instancia rápida puede consumir el `eof` rereencolado repetidamente antes de que una instancia más lenta o ocupada tenga la oportunidad de verlo. Con N instancias, las mismas N-1 podrían consumir el `eof` en loop indefinidamente sin que la restante lo reciba nunca. El protocolo no tiene liveness garantizada.