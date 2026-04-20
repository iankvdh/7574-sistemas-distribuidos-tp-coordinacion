## Criterios de Corrección
###  Implica desaprobación:
- Entregar una implementación que no cumpla con las pruebas provistas.
- Forzar el cumplimiento de las pruebas amañando la implementación.
- Realizar una implementación que no aproveche todas las réplicas de los
procesos.
### Implica observación o quita de puntos:
- Diseñar un protocolo de control que se torne inviáble para grandes volúmenes
de datos o de réplicas.


## Requerimientos Funcionales
Se solicita completar la implementación de un sistema distribuído permitiendo
que la solución escale respecto a los clientes a atender y a los recursos /
cantidad de controles que se le puedan dedicar.

Se deberá optar entre o bien completar el trabajo en el lenguaje Python, o bien
en Golang. Elegir solo uno de estos lenguajes.

## Requerimientos No Funcionales
- El sistema debe utilizar buenas prácticas de programación.
- Las adiciones deberán están sujetas a carpetas y archivos concretos que
se detallan en el README del repositorio.
- El sistema deberá hacer uso de tantas réplicas de un control como sean
configuradas.
- El sistema deberá minimizar la redundancia de datos y cómputo, asi como
minimizar la cantidad de información intercambiada entre las instancias.
- Se deberá redactar un pequeño informe explicando los mecanismos de
coordinación implementados.

## Preguntas de compañeros y respuestas de profesores:
**Manejo de Fallas y Casos Borde** 
  - **Criterio de Estabilidad**: Se puede asumir que la comunicación es estable y que los nodos no fallan (no hay caídas de instancias ni pérdidas de red). Si ocurre una desconexión, el sistema puede liberar recursos y terminar la ejecución; no se requiere tolerancia a fallas en esta instancia.

  - **Coordinación y Condición de Carrera en EOF**: Es obligatorio manejar casos borde lógicos.

    - Ejemplo crítico: En un escenario con múltiples réplicas de Sum, si una recibe un EOF y lo notifica a las demás vía fanout, debe asegurarse de que las otras réplicas no hagan el "flush" de datos hasta haber terminado de procesar los mensajes que ya tenían en su cola de trabajo.


## Clientes:
 cada cliente es independiente, cada uno es una consulta distinta que le envía al sistema. El sistema recibe los pares fruta y cantidad, lo ingesta y luego tiene distintas etapas (como una especie de pipeline). 
 1) Primero una etapa de suma (esquema de working queues), en donde si yo recibí manzana 5 y manzana 8 me quedo con manzana 13 y así con cada suma. 
 2) Cuando termina la etapa de suma se envía al nodo de agregaciíon (ya que los mensajes de manzanas pudieron haber quedado en distintos workers) que se encarga de juntar los resultados (esto es como una especie de map reduce en el que necesito que todos los datos de manzana vayan a un mismo lado).
 3) Luego pasa a una de JOIN (esta no es tan evidente o necesaria en este TP) aunque nos va a servir para el próximo. No es 100% necesario hacerla para este TP aunque puede servir.
 4) luego le devolvemos al gateway el top tres de pares fruta cantidad para que este se lo devuelva a cada cliente por separado.

 ## Notas
 - ¿Cuando es que termina el input de datos? ¿Alcanza con que la cola esté vacía? -> NO porque puede pasar que quede vacía mientras se manden mensajes, además inicia vacía.
 - En el gateway tenemos una abstracción por cada cliente (objeto que encapsula por cada cliente la conexion (socker) y ademasi tiene un message_handler que sabe serializar y deserializar)
- cuando tengamos que empezar a manejar a varios clientes vamos a tener que agregar mas logica en message_handler (para que no sea solamente un puente entre protocolo interno y externo)
- queremos una solucion que realmente aprobeche las instancias (ejemplo de suma) y que no haga un trabajo redundante -> la solucion debe escalar (aunque no nos piden que modifiquemos la cantidad de instancias durante la ejecucion) -> no es solo que funcione, sino que aproveche las instancias de queue y no se realicen ineficiencias ni trabajo redundante.
- En commons podemos poner lo que querramos
- La implementacion base tiene limitaciones adrede. Hay que mejorarla
- Aunque por cuestiones del GIL de python quizas debamos usar multiproces en lugar de hilos: Ojo como pasamos los sockets entre threads (tener cuidado con escribir de dos threads distintos o separas la conexión de rabbir en dos threads distintos).

## Prohibido modificar:
- Archivos csv
- Clientes
- Socket TCP/IP
- colas donde escribe y lee el gateway (fruit_amount, fruit_top)
- gateway (excepto el message_handler y protocolo interno)
- docker compose (variables de entorno (aunque la cátedra fue generosa y dejó varias en los docker que podemos usar))

## podemos modificar:
- Sum (especialmente hay que modificar (complejizar) handleDataMessage)
- cola fruit_total_amount (actualmente creo que es un echange broadcast, pero podría ser cualquier cosa (topicos, distintas colas, colas hacia si mismo desde el de suma y que se hablen entre ellos, tenemos total libertad  ))
- cola partial_fruit_count (actualmente creo que es un echange broadcast, pero podría ser cualquier cosa (topicos, distintas colas, colas hacia si mismo desde el de suma y que se hablen entre ellos, tenemos total libertad  ))
- Agregation
- Join
- structura de como se envian los datos en el protocolo interno (actualemte es un json "[fruta, cantidad]" y el final del archivo es una lista vacia "[]")

## Variables de entorno de los docker que podemos usar (revisar si hay más en los yaml de los scenarios) -> no es necesario usar todas, pero el agregation prefix si se debe usar para las conexiones que se hacen (el scnerio 5 los nombres son cualquier cosa no se entiende nada pero debe funcionar -> componer los nombres de lo que querramos en base a las variables)
    environment:
    - AGGREGATION_AMOUNT=3
    - AGGREGATION_PREFIX=aggregation
    - ID=1
    - MOM_HOST=rabbitmq
    - OUTPUT_QUEUE=join_queue
    - PYTHONUNBUFFERED=1
    - SUM_AMOUNT=3
    - SUM_PREFIX=sum
    - TOP_SIZE=3

## Qué libreriías podemos usar
- json 
- struct 
- preguntar por otras 


## Informe:
Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.


## AL MOMENTO DE PROBAR:
los pofes van a darle un nuevo docker compose con una cantidad indefinida de elementos de suma y de agregation con un nombre completamente al azar, nos van a cambiar la implementacion de los fruit items por lo que cambia el criterio de cual es la fruta mas grande o mas chica y ademas nos cambian los datos de entrada. Y así al ejecutar nuestro código debe dar el mismo resultado que al ejecutarlo serialmente.



## Errores comunes en TPs anteriores
### 1. Gestión de Memoria y Escalabilidad (O(1) siempre)
* **❌ ERROR:** Cargar archivos completos en memoria (`ReadAll`, `list(generator)`, `readlines()`).
* **❌ ERROR:** Leer línea por línea pero acumular los objetos en una lista/slice antes de procesar.
* **✅ REGLA:** Usar **Streaming**. El consumo de RAM debe ser constante e independiente del tamaño del dataset.
* **✅ REGLA (Go):** Usar `csv.Reader.Read()` en un loop y procesar/enviar inmediatamente.
* **✅ REGLA (Python):** Cuando tenemos generadores (`yield`) **nunca** castearlos a `list()`.

### 2. Protocolo y Robustez de Red
* **❌ ERROR:** Hardcodear constantes del protocolo (ej. `MaxPayloadSize = 8192`).
* **✅ REGLA:** Todo parámetro debe ser configurable vía archivo (`.yaml`/`.ini`) o variables de entorno.
* **❌ ERROR:** Asumir que `send()` o `recv()` entregan todo de una vez (*Short Reads/Writes*).
* **✅ REGLA:** Implementar **Framing** (Header de longitud Big-Endian + Payload).
* **✅ REGLA:** Implementar funciones `read_exact(n)` y `write_exact(data)` que usen bucles hasta completar los bytes esperados.


### 3. Manejo de Conexiones y Servidor
* **❌ ERROR:** Hacer **Polling** con `timeout` (ej. `sock.settimeout(1)` para chequear un flag). Es ineficiente en CPU.
* **✅ REGLA:** El servidor debe ser bloqueante. Para apagarlo, el handler de señales (SIGTERM) debe cerrar el socket del servidor para destrabar el `accept()`.
* **❌ ERROR:** Crear una conexión nueva para cada mensaje pequeño.
* **✅ REGLA:** Reutilizar conexiones (**Conexiones Persistentes**).

### 4. Graceful Shutdown (Cierre Limpio)
* **❌ ERROR:** Usar `os.exit()`, `sys.exit()` o `process.kill()` sin intentar un cierre coordinado.
* **❌ ERROR:** No notificar a los procesos/hilos hijos del cierre del padre.
* **✅ REGLA (Cascada de Cierre):**
    1. El Padre recibe `SIGTERM`.
    2. El Padre **cierra los sockets de los clientes**.
    3. Los Hijos detectan el cierre del socket (**EOF**), salen de su loop y limpian recursos.
    4. El Padre espera a los hijos con `join(timeout)` y finalmente usa `terminate()` (ajustar según se use `multiprocessing` o `threading`).
* **✅ REGLA:** Usar bloques `finally` (Python) o `defer` (Go) para asegurar el cierre de recursos.


### 5. Concurrencia y Secciones Críticas
* **❌ ERROR:** Asumir que una etapa de "lectura" no necesita Locks si hay procesos concurrentes (especialmente si el recurso es un archivo en disco).
* **✅ REGLA:** Proteger accesos compartidos (archivos, memoria compartida, diccionarios de Manager) con **Locks**.
* **✅ REGLA (Python):** Usar `multiprocessing` para paralelismo real de CPU y `threading` solo para tareas bloqueantes (ej. I/O). Si se usa `threading`, justificar en el README por qué el GIL no afecta el desempeño.


### 6. Diseño de Middleware y Sincronización
* **❌ ERROR:** Poner lógica de sincronización entre componentes en los tests (ej. polling a una API para esperar que existan bindings).
* **✅ REGLA:** Toda lógica de coordinación y sincronización debe vivir en el middleware, que es el componente que abstrae la comunicación. Si en un test necesitás esperar a que algo esté listo antes de continuar, es señal de que el middleware debería exponer ese mecanismo.
* **⚠️ SEÑAL DE ALERTA:** Si te encontrás agregando lógica de espera/polling en un test, evaluá exhaustivamente si no hay una solución mejor en el componente que estás probando.


### 7. Prácticas Generales de Ingeniería
* **✅ REGLA:** Mantener el `accept()` y el `close()` del socket principal en el mismo scope de ejecución para facilitar la trazabilidad.
* **✅ REGLA:** Loguear siempre: `acción | resultado | contexto (ID cliente/agencia)`. Evitar el uso de `print()`.
* **✅ REGLA:** Al enviar mensajes críticos, esperar una confirmación de aplicación (`ACK`) para asegurar la persistencia en el otro extremo antes de continuar.

