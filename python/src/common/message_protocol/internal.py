import json


class Kind:
    DATA = "data"
    EOF = "eof"
    RING_TOKEN = "ring_token"
    RING_FINISH = "ring_finish"
    SUM_PARTIAL = "sum_partial"
    SUM_DONE = "sum_done"
    AGG_TOP = "agg_top"
    FINAL_TOP = "final_top"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
