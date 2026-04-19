from uuid import uuid4
from common import message_protocol


class MessageHandler:

    def __init__(self):
        self._client_id = uuid4().hex
        self._data_count = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self._data_count += 1
        return message_protocol.internal.serialize(
            {
                "kind": "data",
                "client_id": self._client_id,
                "fruit": fruit,
                "amount": int(amount),
            }
        )

    def serialize_eof_message(self, _):
        return message_protocol.internal.serialize(
            {
                "kind": "eof",
                "client_id": self._client_id,
                "total_messages": self._data_count,
            }
        )

    def deserialize_result_message(self, message):
        msg = message_protocol.internal.deserialize(message)
        if msg.get("kind") != "final_top" or msg.get("client_id") != self._client_id:
            return None
        return msg["top"]
