import os  # Required for path operations
from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from mcp import StdioServerParameters
from decouple import config
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner
from google.genai import types
import asyncio
import logging
from dotenv import load_dotenv
from google.adk.models.lite_llm import LiteLlm
from omnidaemon import OmniDaemonSDK, start_api_server, AgentConfig, SubscriptionConfig

load_dotenv()
# api_key = os.getenv("GOOGLE_API_KEY")
api_key = os.getenv("OPENAI_API_KEY")


sdk = OmniDaemonSDK()


logger = logging.getLogger(__name__)


TARGET_FOLDER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "/home/abiorh/ai/google_adk_file_system"
)


filesystem_agent = LlmAgent(
    # model="gemini-2.0-flash",
    model=LiteLlm(model="openai/gpt-4.1"),
    # model=LiteLlm(model="gemini/gemini-2.0-flash"),
    name="filesystem_assistant_agent",
    instruction="Help the user manage their files. You can list files, read files, etc.",
    tools=[
        McpToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command="npx",
                    args=[
                        "-y",
                        "@modelcontextprotocol/server-filesystem",
                        os.path.abspath(TARGET_FOLDER_PATH),
                    ],
                ),
                timeout=60,
            ),
        )
    ],
)


# --- Session Management ---
# Key Concept: SessionService stores conversation history & state.
# InMemorySessionService is simple, non-persistent storage for this tutorial.
session_service = InMemorySessionService()

# Define constants for identifying the interaction context
APP_NAME = "filesystem_agent"
USER_ID = "user_1"
SESSION_ID = "session_001"


# Create the specific session where the conversation will happen
async def create_session():
    await session_service.create_session(
        app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID
    )


# --- Runner ---
# Key Concept: Runner orchestrates the agent execution loop.
runner = Runner(
    agent=filesystem_agent, app_name=APP_NAME, session_service=session_service
)
