import os
import signal
import logging
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

Kind = message_protocol.internal.Kind


def _default_meta():
    return {
        "count": 0,
        "is_leader": False,
        "total_messages": None,
    }


def _aggregator_id(fruit):
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class InputWorker:

    def __init__(self, meta_by_client, partials_by_client, lock):
        self._meta_by_client = meta_by_client
        self._partials_sum_by_client = partials_by_client
        self._lock = lock
        self._shutdown_requested = False
        self.input_queue = None
        self.next_ring_queue = None

    def _handle_data(self, msg):
        cid = msg["client_id"]
        fruit = msg["fruit"]
        amount = int(msg["amount"])
        with self._lock:
            meta = dict(self._meta_by_client.get(cid, _default_meta()))
            partial_by_fruit = dict(self._partials_sum_by_client.get(cid, {}))
            incoming_fruit_item = fruit_item.FruitItem(fruit, amount)
            current = partial_by_fruit.get(fruit)
            if current is None:
                current = fruit_item.FruitItem(fruit, 0)
            meta["count"] += 1
            partial_by_fruit[fruit] = (
                current + incoming_fruit_item
            )  # suma de objetos FruitItem
            self._meta_by_client[cid] = meta
            self._partials_sum_by_client[cid] = partial_by_fruit

    def _handle_eof(self, msg):
        cid = msg["client_id"]
        total_messages = msg["total_messages"]
        with self._lock:
            meta = dict(self._meta_by_client.get(cid, _default_meta()))
            meta["is_leader"] = True
            meta["total_messages"] = total_messages
            count = meta["count"]
            self._meta_by_client[cid] = meta

        # Al llegarme un EOF, inicio el token ring con el conteo de mensajes que tengo para ese cliente.
        self.next_ring_queue.send(
            message_protocol.internal.serialize(
                {
                    "kind": Kind.RING_TOKEN,
                    "client_id": cid,
                    "accumulated_count": count,
                }
            )
        )
        logging.info(f"ring_token init | cid={cid} | sum_id={ID} | count={count}")

    def _process_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == Kind.DATA:
            self._handle_data(msg)
        elif kind == Kind.EOF:
            self._handle_eof(msg)
        else:
            logging.warning(f"input_worker | unexpected kind={kind}")
        ack()

    def _handle_sigterm(self, signum, frame):
        logging.info(f"sigterm | component=input_worker | id={ID}")
        self._shutdown_requested = True
        if self.input_queue:
            self.input_queue.stop_consuming()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        if self._shutdown_requested:
            return
        try:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            self.next_ring_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_ring_{(ID + 1) % SUM_AMOUNT}"
            )
            self.input_queue.start_consuming(self._process_message)
        except Exception as e:
            logging.error(f"input_worker crashed | id={ID} | error={e}")
            raise
        finally:
            if self.input_queue:
                self.input_queue.close()
            if self.next_ring_queue:
                self.next_ring_queue.close()


class RingWorker:

    def __init__(self, meta_by_client, partials_by_client, lock):
        self._meta_by_client = meta_by_client
        self._partials_sum_by_client = partials_by_client
        self._lock = lock
        self._shutdown_requested = False
        self.ring_queue = None
        self.next_ring_queue = None
        self.input_queue_send = None
        self.aggregator_queues = []

    def _handle_ring_token(self, msg):
        cid = msg["client_id"]
        accumulated = msg["accumulated_count"]
        with self._lock:
            meta = dict(self._meta_by_client.get(cid, _default_meta()))
            is_leader = meta["is_leader"]
            count = meta["count"]
            total_messages = meta["total_messages"]

        if not is_leader:
            # Si no soy el líder propago el acumulado por cliente sumando lo que yo acumulé.
            self.next_ring_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": Kind.RING_TOKEN,
                        "client_id": cid,
                        "accumulated_count": accumulated + count,
                    }
                )
            )
            return

        # SOY EL LIDER
        ## valido que el acumulado total sea el que debe ser y mando por el ring el mensaje de FLUSH.
        total = accumulated
        if total == total_messages:
            self.next_ring_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": Kind.RING_FINISH,
                        "client_id": cid,
                    }
                )
            )
            logging.info(f"ring_finish init | cid={cid} | sum_id={ID} | total={total}")

        # faltó contar un mensaje, reencolo EOF para reiniciar el proceso.
        elif total < total_messages:
            with self._lock:
                meta = dict(self._meta_by_client.get(cid, _default_meta()))
                meta["is_leader"] = False
                meta["total_messages"] = None
                self._meta_by_client[cid] = meta
            self.input_queue_send.send(
                message_protocol.internal.serialize(
                    {
                        "kind": Kind.EOF,
                        "client_id": cid,
                        "total_messages": total_messages,
                    }
                )
            )
            logging.info(f"ring retry | cid={cid} | sum_id={ID} | got={total}")

        # se contó demás, hay un error en alguna parte del ring (esto no debería pasar jamas)
        else:
            logging.error(
                f"ring invariant violated | cid={cid} | sum_id={ID} | got={total} > expected={total_messages}"
            )
            raise RuntimeError(
                f"ring invariant violated: accumulated {total} > total_messages {total_messages}"
            )

    def _handle_ring_finish(self, msg):
        cid = msg["client_id"]
        with self._lock:
            meta = dict(self._meta_by_client.get(cid, _default_meta()))
            is_leader = meta["is_leader"]
            partial_by_fruit = dict(self._partials_sum_by_client.get(cid, {}))
            if cid in self._meta_by_client:
                del self._meta_by_client[cid]
            if cid in self._partials_sum_by_client:
                del self._partials_sum_by_client[cid]

        for final_fruit_item in partial_by_fruit.values():
            aggregator_id = _aggregator_id(final_fruit_item.fruit)
            self.aggregator_queues[aggregator_id].send(
                # le envío al aggregator que corresponda mis resultados para ese cliente.
                message_protocol.internal.serialize(
                    {
                        "kind": Kind.SUM_PARTIAL,
                        "client_id": cid,
                        "fruit": final_fruit_item.fruit,
                        "amount": final_fruit_item.amount,
                    }
                )
            )

        # le comunico a todos los aggregators que terminé con ese cliente.
        done_msg = message_protocol.internal.serialize(
            {
                "kind": Kind.SUM_DONE,
                "client_id": cid,
                "src_id": ID,
            }
        )
        for q in self.aggregator_queues:
            q.send(done_msg)
        logging.info(
            f"flush | cid={cid} | sum_id={ID} | frutas={len(partial_by_fruit)}"
        )

        if not is_leader:
            # propago el mensaje de FLUSH por el anillo.
            self.next_ring_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": Kind.RING_FINISH,
                        "client_id": cid,
                    }
                )
            )

    def _process_ring_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == Kind.RING_TOKEN:
            self._handle_ring_token(msg)
        elif kind == Kind.RING_FINISH:
            self._handle_ring_finish(msg)
        else:
            logging.warning(f"ring_worker | unexpected kind={kind}")
        ack()

    def _handle_sigterm(self, signum, frame):
        logging.info(f"sigterm | component=ring_worker | id={ID}")
        self._shutdown_requested = True
        if self.ring_queue:
            self.ring_queue.stop_consuming()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        if self._shutdown_requested:
            return
        try:
            self.ring_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_ring_{ID}"
            )
            self.next_ring_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_ring_{(ID + 1) % SUM_AMOUNT}"
            )
            self.input_queue_send = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            self.aggregator_queues = [
                middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, f"{AGGREGATION_PREFIX}_{i}"
                )
                for i in range(AGGREGATION_AMOUNT)
            ]
            self.ring_queue.start_consuming(self._process_ring_message)
        except Exception as e:
            logging.error(f"ring_worker crashed | id={ID} | error={e}")
            raise
        finally:
            if self.ring_queue:
                self.ring_queue.close()
            if self.next_ring_queue:
                self.next_ring_queue.close()
            if self.input_queue_send:
                self.input_queue_send.close()
            for q in self.aggregator_queues:
                q.close()
