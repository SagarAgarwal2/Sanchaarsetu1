"""In-process stand-in for a Kafka broker, used only for local tests.

Each topic is an append-only list; each consumer group keeps its own offset,
so every group sees every message once, in order.
"""
import asyncio
from types import SimpleNamespace


class _Broker:
    def __init__(self):
        self.logs: dict[str, list[bytes]] = {}
        self.cond = asyncio.Condition()


BROKER = _Broker()


class FakeProducer:
    def __init__(self, *a, **kw):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic, value):
        async with BROKER.cond:
            BROKER.logs.setdefault(topic, []).append(value)
            BROKER.cond.notify_all()


class FakeConsumer:
    def __init__(self, topic, *a, group_id=None, **kw):
        self.topic = topic
        self.offset = 0

    async def start(self):
        pass

    async def stop(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        async with BROKER.cond:
            await BROKER.cond.wait_for(lambda: len(BROKER.logs.get(self.topic, [])) > self.offset)
            value = BROKER.logs[self.topic][self.offset]
            self.offset += 1
        return SimpleNamespace(value=value)
