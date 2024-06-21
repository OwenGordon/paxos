import httpx
import asyncio
import time
from uuid import uuid4
import datetime


DB_NODES = 2
KEY = "foo"
LOG_SERVER = "http://localhost:8000/log/"


async def set_key_async(key: str, value: str, client_id: int) -> httpx.Response:
    return set_key(key, value, client_id)

async def run_client(client_id: int, key: str, values: list[str]):
    for value in values:
        await set_key_async(key, value, client_id)
        await asyncio.sleep(0.4)


def test_set_get_consistency():
    items = [(KEY, "bazz"), (KEY, "bar")]
    responses = []

    tasks = [run_client(1, KEY, ["bar", "bazz"]), run_client(2, KEY, ["foo"])]
    asyncio.run(asyncio.wait(tasks))

    # assert len(set([response.status_code == 200 for response in responses] + [True])) == 1

    responses = []
    for _ in range(DB_NODES):
        responses.append(get_key(KEY, client_id=1))
        time.sleep(0.1)

    print([response.json() for response in responses])
    values = [response.json() for response in responses]
    assert len(set(values)) == 1


def set_key(key: str, value: str, client_id: int = 1) -> httpx.Response:
    message_id = str(uuid4())

    httpx.post(LOG_SERVER,
        json={
                "timestamp": datetime.datetime.now().isoformat(),
                "sender": f"client-{client_id}",
                "receiver": "",
                "action": f"Client set key {key} = {value}",
                "message_id": message_id
        }
    )

    response = httpx.post(
        "http://localhost:6969/set",
        headers={
            "Content-Type": "application/json",
            "Consistency-Model": "eventual",
            "Client-Id": str(client_id),
            "Message-Id": message_id
        },
        json={"key": key, "value": value},
    )

    response_id = response.headers["Message-Id"]

    httpx.post(LOG_SERVER, 
        json={
                "timestamp": datetime.datetime.now().isoformat(),
                "sender": "",
                "receiver": f"client-{client_id}",
                "action": f"Client received OK",
                "message_id": response_id
        }
    )

    return response

def get_key(key: str, client_id: int = 1) -> httpx.Response:
    message_id = str(uuid4())

    httpx.post(LOG_SERVER, 
        json={
                "timestamp": datetime.datetime.now().isoformat(),
                "sender": f"client-{client_id}",
                "receiver": "",
                "action": f"Client get key {key}",
                "message_id": message_id
        }
    )

    response = httpx.get(
        f"http://localhost:6969/get/{KEY}", 
        headers={
            "Client-Id": str(client_id),
            "Message-Id": message_id
        }
    )

    response_id = response.headers["Message-Id"]

    httpx.post(LOG_SERVER, 
        json={
                "timestamp": datetime.datetime.now().isoformat(),
                "sender": "",
                "receiver": f"client-{client_id}",
                "action": f"Client received key {key} = {response.json()}",
                "message_id": response_id
        }
    )

    return response


if __name__ == "__main__":
    test_set_get_consistency()
