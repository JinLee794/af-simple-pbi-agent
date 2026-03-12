"""
Azure Portfolio Review Agent — analyzes ACR, pipeline, and opportunity data
from MSA_AzureConsumption_Enterprise via Power BI MCP.
Uses Microsoft Agent Framework with Azure AI Foundry.
"""

import asyncio
import os

from dotenv import load_dotenv

load_dotenv(override=True)

from azure.identity.aio import DefaultAzureCredential
from agent_framework import Agent, AgentSession
from agent_framework.azure import AzureAIClient
from azure.ai.agentserver.agentframework import from_agent_framework

from tools import (
    lookup_prevalidated_dax,
    get_semantic_model_schema,
    build_custom_dax_query,
)
from providers import CosmosSessionProvider

# Configure these for your Foundry project
PROJECT_ENDPOINT = os.getenv("PROJECT_ENDPOINT")
MODEL_DEPLOYMENT_NAME = os.getenv("MODEL_DEPLOYMENT_NAME", "gpt-4.1-mini")


AGENT_INSTRUCTIONS = """You are an Azure portfolio review assistant using the MSA_AzureConsumption_Enterprise Power BI model.

You answer questions about a user's Azure portfolio: gap-to-target, opportunity conversion ranking, and recommended actions.

## Tool Strategy — Use in This Order

### 1. Pre-validated queries (fast path, preferred)
Call `lookup_prevalidated_dax` with an intent to get a battle-tested DAX query:
- `auth_check` → verify Power BI connectivity (always run first)
- `gap_to_target` → ACR actuals + budget + pipeline summary (2 queries)
- `opportunity_conversion` → opportunity detail + scoring rubric
- `account_recommendations` → account attributes + recommendation rules

All intents accept the same scope params (tpids, account_names, solution_play, strategic_pillar).
You can call multiple intents **in parallel** with the same scope.

### 2. Schema discovery (when pre-validated queries don't cover the question)
Call `get_semantic_model_schema` to get the full model schema — tables, columns, measures, and relationships.
Use this to understand what data is available before writing a custom query.

### 3. Custom query builder (structured DAX assembly)
Call `build_custom_dax_query` with the table/column/measure references you found in the schema.
This assembles syntactically valid DAX — you provide structured inputs, it handles the syntax.

**Never write raw DAX yourself** — always use either the pre-validated catalog or the builder.

## Workflow

1. **Auth**: `lookup_prevalidated_dax(intent="auth_check")`
2. **Scope**: Ask the user for accounts, filters, and time window
3. **Query**: Call the appropriate tool(s) to get DAX, then execute via Power BI MCP ExecuteQuery
4. **Analyze**: Apply classification rules and scoring from the tool responses
5. **Report**: Present Portfolio Summary → Gap Analysis → Top Opportunities → Recommended Actions → Scope & Freshness
"""


def _create_agent(client, cosmos_provider: CosmosSessionProvider):
    return Agent(
        client=client,
        name="AzurePortfolioReviewAgent",
        instructions=AGENT_INSTRUCTIONS,
        tools=[
            lookup_prevalidated_dax,
            get_semantic_model_schema,
            build_custom_dax_query,
        ],

        context_providers=[cosmos_provider],
    )


async def main_server():
    """Run the agent as a web server."""
    cosmos_provider = CosmosSessionProvider()
    async with (
        DefaultAzureCredential() as credential,
        AzureAIClient(
            project_endpoint=PROJECT_ENDPOINT,
            model_deployment_name=MODEL_DEPLOYMENT_NAME,
            credential=credential,
        ) as client,
    ):
        try:
            agent = _create_agent(client, cosmos_provider)
            print("Azure Portfolio Review Agent running on http://localhost:8088")
            server = from_agent_framework(agent)
            await server.run_async()
        finally:
            await cosmos_provider.close()


async def main_interactive():
    """Run the agent interactively in the terminal."""
    cosmos_provider = CosmosSessionProvider()
    async with (
        DefaultAzureCredential() as credential,
        AzureAIClient(
            project_endpoint=PROJECT_ENDPOINT,
            model_deployment_name=MODEL_DEPLOYMENT_NAME,
            credential=credential,
        ) as client,
    ):
        try:
            agent = _create_agent(client, cosmos_provider)
            session = AgentSession()

            print("Azure Portfolio Review Agent (interactive mode)")
            print("Type your message and press Enter. Type 'quit' or 'exit' to stop.\n")

            while True:
                try:
                    user_input = input("You: ")
                except (EOFError, KeyboardInterrupt):
                    print("\nGoodbye!")
                    break

                if user_input.strip().lower() in ("quit", "exit"):
                    print("Goodbye!")
                    break

                if not user_input.strip():
                    continue

                response = await agent.run(user_input, session=session)
                print(f"\nAgent: {response.text}\n")
        finally:
            await cosmos_provider.close()


if __name__ == "__main__":
    import sys

    if "--serve" in sys.argv:
        asyncio.run(main_server())
    else:
        asyncio.run(main_interactive())
