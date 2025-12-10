"""Storage-based supervisor tracking utilities.

These utilities replace the in-memory supervisor registry with storage-based
tracking, allowing supervisor state to:
- Survive process crashes/restarts
- Be accessible from any process with storage access
- Provide historical supervisor data
"""

import logging
from typing import Dict, Optional
from omnidaemon.storage import store as default_store

logger = logging.getLogger(__name__)

store = default_store


async def get_supervisor_state(agent_name: str) -> Optional[Dict]:
    """Get supervisor state from storage.

    Args:
        agent_name: Name of the agent

    Returns:
        Supervisor state dict with:
        - agent_name: str
        - state: str (IDLE, RUNNING, CRASHED, etc.)
        - healthy: bool
        - pid: int or None
        - start_time: float
        - cpu_percent: float
        - memory_mb: float
        - total_requests: int
        - restart_count: int
        - last_heartbeat: float
        - last_updated: float
    """
    return await store.get_config(f"supervisor:{agent_name}", default=None)


async def list_all_supervisors() -> Dict[str, Dict]:
    """List all supervisor states from storage.

    Returns:
        Dict mapping agent_name to supervisor state
    """
    supervisor_names = await store.get_config("_supervisor_registry", default=[])

    logger.info(
        f"list_all_supervisors: Registry has {len(supervisor_names)} entries: {supervisor_names}"
    )

    result = {}
    for agent_name in supervisor_names:
        state = await get_supervisor_state(agent_name)
        if state:
            result[agent_name] = state
            logger.debug(f"list_all_supervisors: Found state for {agent_name}")
        else:
            logger.warning(
                f"list_all_supervisors: No state found for {agent_name} (in registry but no data)"
            )

    return result


async def register_supervisor_in_storage(agent_name: str) -> None:
    """Register supervisor name in storage registry.

    This tracks which supervisors exist for list_all_supervisors.
    Called when a supervisor is created.

    Args:
        agent_name: Name of the agent
    """
    registry = await store.get_config("_supervisor_registry", default=[])
    if agent_name not in registry:
        registry.append(agent_name)
        await store.save_config("_supervisor_registry", registry)


async def unregister_supervisor_from_storage(agent_name: str) -> None:
    """Remove supervisor from storage registry.

    Called when a supervisor is explicitly deleted (not on crash).

    Args:
        agent_name: Name of the agent
    """
    registry = await store.get_config("_supervisor_registry", default=[])
    if agent_name in registry:
        registry.remove(agent_name)
        await store.save_config("_supervisor_registry", registry)

    await store.save_config(f"supervisor:{agent_name}", None)
