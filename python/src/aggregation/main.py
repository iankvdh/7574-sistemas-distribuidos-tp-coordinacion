import os
import signal
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.sessions = {}

    def _get_session(self, client_id):
        if client_id not in self.sessions:
            self.sessions[client_id] = {"fruits": {}, "done_count": 0}
        return self.sessions[client_id]

    def _handle_sum_partial(self, msg):
        cid = msg["client_id"]
        fruit = msg["fruit"]
        session = self._get_session(cid)
        session["fruits"][fruit] = session["fruits"].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(msg["amount"]))

    def _handle_sum_done(self, msg):
        cid = msg["client_id"]
        session = self._get_session(cid)
        session["done_count"] += 1
        if session["done_count"] < SUM_AMOUNT:
            return

        sorted_fruits = sorted(session["fruits"].values())
        top_fruits = reversed(sorted_fruits[-TOP_SIZE:])
        top = []

        for fruit_chunk in top_fruits:
            top.append([fruit_chunk.fruit, fruit_chunk.amount])

        self.output_queue.send(
            message_protocol.internal.serialize(
                {
                    "kind": "agg_top",
                    "client_id": cid,
                    "src_id": ID,
                    "top": top,
                }
            )
        )
        logging.info(f"agg_top | cid={cid} | agg_id={ID} | top={top}")
        del self.sessions[cid]

    def _process_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == "sum_partial":
            self._handle_sum_partial(msg)
        elif kind == "sum_done":
            self._handle_sum_done(msg)
        else:
            logging.warning("aggregation | unknown kind=%s", kind)
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            self.input_queue.start_consuming(self._process_message)
        finally:
            self.input_queue.close()
            self.output_queue.close()

    def _handle_sigterm(self, signum, frame):
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
