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

        for i in range(AGGREGATION_AMOUNT):
            data_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{AGGREGATION_PREFIX}_{i}"
            )
            self.aggregator_queues.append(data_output_queue)
        self.sessions = {}

    def _get_session(self, client_id):
        if client_id not in self.sessions:
            self.sessions[client_id] = {"partial_by_fruit": {}}
        return self.sessions[client_id]

    def _handle_data(self, msg):
        cid = msg["client_id"]
        fruit = msg["fruit"]
        session = self._get_session(cid)
        session["partial_by_fruit"][fruit] = session["partial_by_fruit"].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(msg["amount"]))

    def _handle_eof(self, msg):
        cid = msg["client_id"]
        session = self._get_session(cid)
        for final_fruit_item in session["partial_by_fruit"].values():
            agregator_id = _aggregator_id(final_fruit_item.fruit)
            self.aggregator_queues[agregator_id].send(
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

    def _process_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == "data":
            self._handle_data(msg)
        elif kind == "eof":
            self._handle_eof(msg)
        else:
            logging.warning("sum | unknown kind=%s", kind)
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            self.input_queue.start_consuming(self._process_message)
        finally:
            self.input_queue.close()
            for q in self.aggregator_queues:
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
