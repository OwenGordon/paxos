import httpx
import asyncio
import time
from uuid import uuid4
import datetime
from enum import Enum


KEY = "foo"
LOG_SERVER = "http://localhost:8000/log/"


async def log_send(client_id, action, message_id):
    async with httpx.AsyncClient() as client:
        await client.post(LOG_SERVER, 
            json={
                    "timestamp": datetime.datetime.now().isoformat(),
                    "sender": f"client-{client_id}",
                    "receiver": "",
                    "action": action,
                    "message_id": message_id
            }
        )


async def log_recv(client_id, action, message_id):
    async with httpx.AsyncClient() as client:
        await client.post(LOG_SERVER, 
            json={
                    "timestamp": datetime.datetime.now().isoformat(),
                    "sender": "",
                    "receiver": f"client-{client_id}",
                    "action": action,
                    "message_id": message_id
            }
        )


class ConsistencyMode(Enum):
    SEQUENTIAL = "sequential"
    EVENTUAL = "eventual"


# Create a synchronization event
start_event = asyncio.Event()


async def client_1():
    # Wait for the event to start
    await start_event.wait()

    for value in ["bar", "bazz"]:
        await set_key(KEY, value, 1, ConsistencyMode.SEQUENTIAL)

    
    response = await get_key(KEY, 1, ConsistencyMode.SEQUENTIAL)
    assert response.json() != "bar"


async def client_2():
    # Wait for the event to start
    await start_event.wait()

    for value in ["foo"]:
        await set_key(KEY, value, 2, ConsistencyMode.SEQUENTIAL)
    
    response = await get_key(KEY, 2, ConsistencyMode.SEQUENTIAL)


async def main():
    # Set up the clients
    task1 = asyncio.create_task(client_1())
    task2 = asyncio.create_task(client_2())
    
    # Allow a short delay to ensure both tasks are waiting on the event
    await asyncio.sleep(0.1)  # Small delay to ensure both tasks reach the event wait

    # Signal both clients to start
    start_event.set()

    # Wait for both clients to complete
    await asyncio.gather(task1, task2)


async def set_key(key: str, value: str, client_id: int = 1, consistency_mode: ConsistencyMode = ConsistencyMode.EVENTUAL) -> httpx.Response:
    message_id = str(uuid4())

    await log_send(client_id, f"Client set key {key} = {value}", message_id)

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:6969/set",
            headers={
                "Content-Type": "application/json",
                "Consistency-Model": consistency_mode.value,
                "Client-Id": str(client_id),
                "Message-Id": message_id
            },
            json={"key": key, "value": value},
        )

    response_id = response.headers["Message-Id"]

    await log_recv(client_id, f"Client received OK", response_id)

    return response


async def get_key(key: str, client_id, consistency_mode) -> httpx.Response:
    message_id = str(uuid4())

    await log_send(client_id, f"Client get key {key}", message_id)

    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"http://localhost:6969/get/{KEY}", 
            headers={
                "Client-Id": str(client_id),
                "Message-Id": message_id,
                "Consistency-Model": consistency_mode.value,
            }
        )

    response_id = response.headers["Message-Id"]

    await log_recv(client_id, f"Client received key {key} = {response.json()}", response_id)

    return response


if __name__ == "__main__":
    asyncio.run(main())
