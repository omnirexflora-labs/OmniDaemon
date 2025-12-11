"""
Supervised Agent Runner - Manages agent lifecycle with process isolation

This runner uses supervisors to manage each agent in a separate process, providing:
- Automatic process spawning and monitoring
- Crash recovery and automatic restarts
- Process isolation (one agent crash doesn't affect others)
- Independent resource management per agent
- IPC handling between main process and agent processes

For simple same-process agents without lifecycle management,
use OmniDaemonSDK directly (no supervisor needed).
"""

import asyncio
import logging
import sys
from typing import Any, Dict
from decouple import config

from omnidaemon import OmniDaemonSDK, start_api_server, AgentConfig, SubscriptionConfig
from omnidaemon.agent_runner.agent_supervisor_runner import (
    create_supervisor_from_directory,
    shutdown_all_supervisors,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)
sdk = OmniDaemonSDK()

# Supervisors
_google_adk_supervisor = None


# NOTE agent directory must be given as absolute path or relative to current working directory
async def get_google_adk_supervisor():
    global _google_adk_supervisor
    if _google_adk_supervisor is None:
        _google_adk_supervisor = await create_supervisor_from_directory(
            agent_name="google_adk_filesystem_agent_with_supervisor",
            agent_dir="./google_adk_with_supervisor",
            callback_function="call_google_adk_agent_with_supervisor",
        )
    return _google_adk_supervisor


async def call_google_adk_agent_with_supervisor(
    message: Dict[str, Any],
) -> Dict[str, Any]:
    supervisor = await get_google_adk_supervisor()
    logger.info(f"Google ADK agent received: {message}")
    result = await supervisor.handle_event(message)
    logger.info(f"Google ADK agent result: {result}")
    return result


async def call_response(message: Dict[str, Any]) -> None:
    logger.info(f"Response received: {message}")


async def main():
    try:
        logger.info("Pre-initializing supervisors...")
        await get_google_adk_supervisor()
        logger.info("Supervisors initialized")

        logger.info("Registering agents...")

        await sdk.register_agent(
            agent_config=AgentConfig(
                name="google_adk_filesystem_agent_with_supervisor",
                topic="file_system.google_adk.tasks",
                callback=call_google_adk_agent_with_supervisor,
                description="Filesystem helper via supervised Google ADK agent.",
                tools=[],
                config=SubscriptionConfig(
                    reclaim_idle_ms=6000, dlq_retry_limit=3, consumer_count=3
                ),
            )
        )

        await sdk.register_agent(
            agent_config=AgentConfig(
                name="google_adkagent_response",
                topic="file_system.response",
                callback=call_response,
                description="Response from file system agent.",
                tools=[],
                config=SubscriptionConfig(
                    reclaim_idle_ms=6000, dlq_retry_limit=3, consumer_count=1
                ),
            )
        )

        logger.info("All agents registered")
        logger.info("Starting OmniDaemon...")
        await sdk.start()

        if config("OMNIDAEMON_API_ENABLED", default=False, cast=bool):
            api_port = config("OMNIDAEMON_API_PORT", default=8765, cast=int)
            asyncio.create_task(start_api_server(sdk, host="0.0.0.0", port=api_port))
            logger.info(f"API running on http://0.0.0.0:{api_port}")

        logger.info("Processing events. Press Ctrl+C to stop.")
        await asyncio.Event().wait()

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown signal received")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down...")
        await sdk.shutdown()
        await shutdown_all_supervisors()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
