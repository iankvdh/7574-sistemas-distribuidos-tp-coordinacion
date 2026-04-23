import os
import signal
import logging
import heapq

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
Kind = message_protocol.internal.Kind
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.sessions = {}
        self._shutdown_requested = False

    def _get_session(self, client_id):
        if client_id not in self.sessions:
            self.sessions[client_id] = {"items": {}, "received": 0}
        return self.sessions[client_id]

    def _handle_agg_top(self, msg):
        cid = msg["client_id"]
        session = self._get_session(cid)
        for fruit, amount in msg["top"]:
            session["items"][fruit] = session["items"].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
        session["received"] += 1
        logging.info(f"agg_top | cid={cid} | received={session['received']}/{AGGREGATION_AMOUNT}")
        if session["received"] < AGGREGATION_AMOUNT:
            return

        top_fruits = heapq.nlargest(TOP_SIZE, session["items"].values())
        top = []
        for fruit_chunk in top_fruits:
            top.append([fruit_chunk.fruit, fruit_chunk.amount])

        self.output_queue.send(
            message_protocol.internal.serialize(
                {
                    "kind": Kind.FINAL_TOP,
                    "client_id": cid,
                    "top": top,
                }
            )
        )
        logging.info(f"final_top | cid={cid} | top={top}")
        del self.sessions[cid]

    def _process_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        kind = msg.get("kind")
        if kind == Kind.AGG_TOP:
            self._handle_agg_top(msg)
        else:
            logging.warning(f"join | unknown kind={kind}")
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        if self._shutdown_requested:
            return
        try:
            self.input_queue.start_consuming(self._process_message)
        finally:
            self.input_queue.close()
            self.output_queue.close()

    def _handle_sigterm(self, signum, frame):
        logging.info("sigterm | component=join")
        self._shutdown_requested = True
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(f"starting | component=join | agg_amount={AGGREGATION_AMOUNT} | top_size={TOP_SIZE}")
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
