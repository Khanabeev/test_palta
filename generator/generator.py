import json
import random

clients = ["foo", "bar", "baz"]
payloads = ["a=1&b=2", json.dumps({"a": 1, "b": 2})]


def generate():
    with open("data/requests.txt", "w") as f:
        for _ in range(1000):
            d = {
                "client_id": clients[random.randint(0, 2)],
                "payload": payloads[random.randint(0, 1)]
            }
            if random.randint(0, 1):
                d.update({"id": random.randint(0, 800)})
            line = json.dumps(d)
            f.write("%s\n" % line)
