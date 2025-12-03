"""
Publisher for Google ADK Agent

Publishes tasks to the file_system.google_adk.tasks topic to test the supervised Google ADK agent.
"""

import asyncio
from omnidaemon import OmniDaemonSDK, EventEnvelope, PayloadBase
from typing import Union

sdk = OmniDaemonSDK()


async def publish_google_adk_task(
    sdk: OmniDaemonSDK, message: Union[str, dict, list, tuple] = None
):
    """
    Publishes a filesystem task to OmniDaemon for Google ADK agent.

    Args:
        sdk: OmniDaemonSDK instance
        message: Optional message to send to the Google ADK agent.
                 Can be str, dict, list, or tuple.
                 If not provided, uses a default test message.
    """
    if message is None:
        message = "List all files in the current directory"

    topic = "file_system.google_adk.tasks"
    event_payload = EventEnvelope(
        topic=topic,
        payload=PayloadBase(
            content=message,  # Content can be str, dict, list, or tuple
            webhook=None,  # Optional: "http://localhost:8004/google_adk_result"
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
    test_message = "List all files in the current directory"

    # Or use command line argument if provided
    import sys

    if len(sys.argv) > 1:
        test_message = " ".join(sys.argv[1:])

    await publish_google_adk_task(sdk, test_message)
    print("\n📤 Task sent! Check the agent runner logs for processing.")


if __name__ == "__main__":
    asyncio.run(main())
