import httpx
import asyncio


async def test_set_get_consistency():
    DB_NODES = 4
    KEY = "a"
    items = [(KEY, "b"), (KEY, "d"), (KEY, "c")]
    tasks = [set_key(key, value) for key, value in items]
    responses = await asyncio.gather(*tasks)
    assert len(set([response.status_code == 200 for response in responses] + [True])) == 1
    # syncronously retrieve gets to test consistency

    child_states = [
        httpx.get(f"http://localhost:6969/get/{KEY}") for _ in range(DB_NODES)
    ]
    values = [child_state.json() for child_state in child_states]
    assert len(set(values)) == 1


async def set_key(key, value):
    async with httpx.AsyncClient() as client:
        return await client.post(
            "http://localhost:6969/set",
            headers={"Content-Type": "application/json"},
            json={"key": key, "value": value},
        )


if __name__ == "__main__":
    asyncio.run(test_set_get_consistency())
