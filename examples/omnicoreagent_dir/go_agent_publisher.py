"""
Publisher for Go Agent

Publishes tasks to the file_system.go.tasks topic to test the supervised Go agent.
"""

import asyncio
from omnidaemon import OmniDaemonSDK, EventEnvelope, PayloadBase
from typing import Union

sdk = OmniDaemonSDK()


async def publish_go_agent_task(
    sdk: OmniDaemonSDK, message: Union[str, dict, list, tuple] = None
):
    """
    Publishes a task to OmniDaemon for Go agent.

    Args:
        sdk: OmniDaemonSDK instance
        message: Optional message to send to the Go agent.
                 Can be str, dict, list, or tuple.
                 If not provided, uses a default test message.
    """
    if message is None:
        message = "tell me what are you and list of tools you have nd what you can do?"

    topic = "file_system.go.tasks"
    event_payload = EventEnvelope(
        topic=topic,
        payload=PayloadBase(
            content=message,  # Content can be str, dict, list, or tuple
            webhook=None,  # Optional: "http://localhost:8004/go_result"
            reply_to="file_system.response",  # Topic for response
        ),
    )

    await sdk.publish_task(event_envelope=event_payload)
    print(f"✅ Task published to topic '{topic}'")
    print(f"   Message: {message}")
    print(f"   Reply will be sent to: {event_payload.payload.reply_to}")


async def main():
    """Main function to publish a test task."""
    # You can customize the message here
    test_message = "Hello from Python! This is a test message for the Go agent."

    # Or use command line argument if provided
    import sys

    if len(sys.argv) > 1:
        test_message = " ".join(sys.argv[1:])

    await publish_go_agent_task(sdk, test_message)
    print("\n📤 Task sent! Check the agent runner logs for processing.")


if __name__ == "__main__":
    asyncio.run(main())
