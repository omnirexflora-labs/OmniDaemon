from adk_agent import runner, create_session, USER_ID, SESSION_ID
from google.genai import types
import logging

logger = logging.getLogger(__name__)


async def call_google_adk_agent_with_supervisor(message: dict):
    """Sends a query to the agent and prints the final response."""
    await create_session()
    query = message.get("content")
    if not query:
        return "No content in the message payload"
    logger.info(f"\n>>> User Query: {query}")

    content = types.Content(role="user", parts=[types.Part(text=query)])

    final_response_text = "Agent did not produce a final response."  # Default

    async for event in runner.run_async(
        user_id=USER_ID, session_id=SESSION_ID, new_message=content
    ):
        if event.is_final_response():
            if event.content and event.content.parts:
                # Assuming text response in the first part
                final_response_text = event.content.parts[0].text
            elif (
                event.actions and event.actions.escalate
            ):  # Handle potential errors/escalations
                final_response_text = (
                    f"Agent escalated: {event.error_message or 'No specific message.'}"
                )
            # Add more checks here if needed (e.g., specific error codes)
            break  # Stop processing events once the final response is found

    print(f"<<< Agent Response: {final_response_text}")
    return final_response_text
