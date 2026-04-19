import os
import zlib
import signal
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _aggregator_id(fruit):
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.aggregator_queues = []
        self.ring_queues = []

        for i in range(AGGREGATION_AMOUNT):
            self.aggregator_queues.append(
                middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, f"{AGGREGATION_PREFIX}_{i}"
                )
            )

        for i in range(SUM_AMOUNT):
            self.ring_queues.append(
                middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, f"{SUM_PREFIX}_ring_{i}"
                )
            )

        self.next_ring_queue = self.ring_queues[(ID + 1) % SUM_AMOUNT]
        self.sessions = {}

    def _get_session(self, client_id):
        if client_id not in self.sessions:
            self.sessions[client_id] = {
                "partial_by_fruit": {},
                "count": 0,
                "is_leader": False,
                "total_messages": None,
            }
        return self.sessions[client_id]

    def _handle_data(self, msg):
        cid = msg["client_id"]
        fruit = msg["fruit"]
        session = self._get_session(cid)
        session["count"] += 1
        session["partial_by_fruit"][fruit] = session["partial_by_fruit"].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(msg["amount"]))

    def _handle_eof(self, msg):
        cid = msg["client_id"]
        session = self._get_session(cid)
        session["is_leader"] = True
        session["total_messages"] = msg["total_messages"]
        self.next_ring_queue.send(
            message_protocol.internal.serialize(
                {
                    "kind": "ring_token",
                    "client_id": cid,
                    "accumulated_count": session["count"],
                }
            )
        )
        logging.info(
            f"ring_token init | cid={cid} | sum_id={ID} | count={session['count']}"
        )

    def _handle_ring_token(self, msg):
        cid = msg["client_id"]
        session = self._get_session(cid)
        accumulated = msg["accumulated_count"]
        if not session["is_leader"]:
            self.next_ring_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": "ring_token",
                        "client_id": cid,
                        "accumulated_count": accumulated + session["count"],
                    }
                )
            )
            return

        # soy el lider
        total = accumulated
        if total == session["total_messages"]:
            self.next_ring_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": "ring_finish",
                        "client_id": cid,
                    }
                )
            )
            logging.info(f"ring_finish init | cid={cid} | sum_id={ID} | total={total}")
        else:
            total_messages = session["total_messages"]
            session["is_leader"] = False
            session["total_messages"] = None
            self.input_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": "eof",
                        "client_id": cid,
                        "total_messages": total_messages,
                    }
                )
            )
            logging.info(f"ring retry | cid={cid} | sum_id={ID} | got={total}")

    def _flush(self, cid):
        session = self.sessions[cid]
        for final_fruit_item in session["partial_by_fruit"].values():
            aggregator_id = _aggregator_id(final_fruit_item.fruit)
            self.aggregator_queues[aggregator_id].send(
                message_protocol.internal.serialize(
                    {
                        "kind": "sum_partial",
                        "client_id": cid,
                        "fruit": final_fruit_item.fruit,
                        "amount": final_fruit_item.amount,
                    }
                )
            )
        done_msg = message_protocol.internal.serialize(
            {
                "kind": "sum_done",
                "client_id": cid,
                "src_id": ID,
            }
        )
        for q in self.aggregator_queues:
            q.send(done_msg)
        logging.info(
            f"flush | cid={cid} | sum_id={ID} | frutas={len(session['partial_by_fruit'])}"
        )
        del self.sessions[cid]

    def _handle_ring_finish(self, msg):
        cid = msg["client_id"]
        is_leader = self._get_session(cid).get("is_leader", False)
        self._flush(cid)
        if not is_leader:
            self.next_ring_queue.send(
                message_protocol.internal.serialize(
                    {
                        "kind": "ring_finish",
                        "client_id": cid,
                    }
                )
            )

    def _process_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == "data":
            self._handle_data(msg)
        elif kind == "eof":
            self._handle_eof(msg)
        else:
            logging.warning(f"sum | unexpected kind on INPUT_QUEUE: {kind}")
        ack()

    def _process_ring_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == "ring_token":
            self._handle_ring_token(msg)
        elif kind == "ring_finish":
            self._handle_ring_finish(msg)
        else:
            logging.warning(f"sum | unexpected kind on ring queue: {kind}")
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue.add_queue_consumer(
            f"{SUM_PREFIX}_ring_{ID}", self._process_ring_message
        )
        try:
            self.input_queue.start_consuming(self._process_message)
        finally:
            self.input_queue.close()
            for q in self.aggregator_queues:
                q.close()
            for q in self.ring_queues:
                q.close()

    def _handle_sigterm(self, signum, frame):
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
