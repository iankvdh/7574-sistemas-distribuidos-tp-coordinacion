import os
import time
import pika
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
)

_MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))
_RETRY_DELAY = 2


def _connect_with_retry(host):
    for attempt in range(_MAX_ATTEMPTS):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError:
            if attempt < _MAX_ATTEMPTS - 1:
                time.sleep(_RETRY_DELAY)
    raise MessageMiddlewareDisconnectedError(
        f"No se pudo conectar a RabbitMQ en {host} tras {_MAX_ATTEMPTS} intentos"
    )


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._connection = None
        self._channel = None
        self._extra_consumers = []
        try:
            self._connection = _connect_with_retry(host)
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=True)
        except Exception as e:
            try:
                self.close()
            except Exception as close_error:
                print(f"Aviso: Error durante la limpieza post-fallo: {close_error}")
            raise MessageMiddlewareMessageError(
                f"Error al inicializar la cola: {e}"
            ) from e

    def close(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar: {e}") from e

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(
                "Conexión perdida con RabbitMQ"
            ) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje: {e}") from e

    def add_queue_consumer(self, queue_name, on_message_callback):
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._extra_consumers.append((queue_name, on_message_callback))

    def start_consuming(self, on_message_callback):
        def make_wrapper(cb):
            def wrapper(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)

                cb(body, ack, nack)

            return wrapper

        try:
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=make_wrapper(on_message_callback),
            )
            for extra_queue, extra_cb in self._extra_consumers:
                self._channel.basic_consume(
                    queue=extra_queue, on_message_callback=make_wrapper(extra_cb)
                )
            self._channel.start_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(
                "Conexión perdida durante el consumo"
            ) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en el consumidor: {e}") from e

    def stop_consuming(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(
                "Conexión perdida al detener el consumo"
            ) from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys)
        self._connection = None
        self._channel = None
        try:
            self._connection = _connect_with_retry(host)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type="direct",
                durable=True,
            )
        except Exception as e:
            try:
                self.close()
            except Exception:
                pass
            raise MessageMiddlewareMessageError(
                f"Error al inicializar Exchange: {e}"
            ) from e

    def close(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar: {e}") from e

    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(
                "Conexión perdida con RabbitMQ"
            ) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al enviar mensaje: {e}") from e

    def start_consuming(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            result = self._channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue
            for key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=queue_name,
                    routing_key=key,
                )
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=queue_name, on_message_callback=callback_wrapper
            )
            self._channel.start_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(
                "Conexión perdida durante el consumo"
            ) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error en el consumidor: {e}") from e

    def stop_consuming(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(
                "Conexión perdida al detener el consumo"
            ) from e
