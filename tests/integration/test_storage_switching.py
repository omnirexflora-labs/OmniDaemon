"""Integration tests for storage backend switching."""

import pytest
import pytest_asyncio
import tempfile
import shutil
import json
from unittest.mock import AsyncMock

from omnidaemon.sdk import OmniDaemonSDK
from omnidaemon.storage.json_store import JSONStore
from omnidaemon.schemas import EventEnvelope, PayloadBase, AgentConfig


@pytest_asyncio.fixture
async def event_bus():
    """Create a mocked event bus that doesn't block."""
    bus = AsyncMock()
    bus.connect = AsyncMock()
    bus.close = AsyncMock()

    async def mock_publish(event_payload, maxlen=None):
        return event_payload.get("id", "task-123")

    bus.publish = AsyncMock(side_effect=mock_publish)

    bus.subscribe = AsyncMock()
    bus.unsubscribe = AsyncMock()
    bus.get_consumers = AsyncMock(return_value={})
    bus._running = False
    bus._connected = True
    yield bus


@pytest.fixture
def storage_dir():
    """Create a temporary storage directory."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestStorageBackendSwitching:
    """Test suite for storage backend switching scenarios."""

    @pytest.mark.asyncio
    async def test_json_to_redis_migration(self, event_bus, storage_dir):
        """Test switching from JSON to Redis storage."""
        json_store = JSONStore(storage_dir=storage_dir)
        await json_store.connect()

        sdk_json = OmniDaemonSDK(event_bus=event_bus, store=json_store)

        async def agent_callback(message):
            return {"processed": True}

        await sdk_json.register_agent(
            AgentConfig(
                topic="migration.topic", callback=agent_callback, name="migration-agent"
            )
        )

        agents = await json_store.list_all_agents()
        assert "migration.topic" in agents
        assert len(agents["migration.topic"]) > 0

        await json_store.close()

        json_store2 = JSONStore(storage_dir=storage_dir)
        await json_store2.connect()

        agents2 = await json_store2.list_all_agents()
        assert "migration.topic" in agents2

        await json_store2.close()

    @pytest.mark.asyncio
    async def test_redis_to_json_migration(self, event_bus, storage_dir):
        """Test switching from Redis to JSON storage."""

        json_store = JSONStore(storage_dir=storage_dir)
        await json_store.connect()

        sdk = OmniDaemonSDK(event_bus=event_bus, store=json_store)

        async def agent_callback(message):
            return {"processed": True}

        await sdk.register_agent(
            AgentConfig(
                topic="migration2.topic",
                callback=agent_callback,
                name="migration2-agent",
            )
        )

        agents = await json_store.list_all_agents()
        assert "migration2.topic" in agents

        await json_store.close()

    @pytest.mark.asyncio
    async def test_data_persistence_across_restarts(self, event_bus, storage_dir):
        """Test data persists across runner restarts."""
        json_store1 = JSONStore(storage_dir=storage_dir)
        await json_store1.connect()

        sdk1 = OmniDaemonSDK(event_bus=event_bus, store=json_store1)

        async def agent_callback(message):
            return {"result": "persisted"}

        await sdk1.register_agent(
            AgentConfig(
                topic="persist.topic", callback=agent_callback, name="persist-agent"
            )
        )

        event = EventEnvelope(
            topic="persist.topic",
            payload=PayloadBase(content=json.dumps({"test": "data"})),
        )
        task_id = await sdk1.publish_task(event)

        await json_store1.save_result(task_id, {"result": "test"})

        await sdk1.shutdown()
        await json_store1.close()

        json_store2 = JSONStore(storage_dir=storage_dir)
        await json_store2.connect()

        sdk2 = OmniDaemonSDK(event_bus=event_bus, store=json_store2)

        agents = await json_store2.list_all_agents()
        assert "persist.topic" in agents

        result = await json_store2.get_result(task_id)
        assert result is not None
        assert result.get("result") == "test"

        await sdk2.shutdown()
        await json_store2.close()
