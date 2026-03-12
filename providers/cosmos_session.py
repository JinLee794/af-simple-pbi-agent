"""
Cosmos DB MongoDB vCluster session lifecycle provider for the Microsoft Agent Framework.

Persists session state to an Azure Cosmos DB MongoDB vCluster collection
so that sessions survive restarts and can be resumed across requests.
Authenticates via Entra ID (MONGODB-OIDC).

Layout:
  - Database:   configurable via COSMOS_DATABASE   (default "agent_sessions")
  - Collection: configurable via COSMOS_COLLECTION  (default "sessions")
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import motor.motor_asyncio
from pymongo.auth_oidc import OIDCCallback, OIDCCallbackContext, OIDCCallbackResult
from azure.identity import DefaultAzureCredential
from agent_framework import AgentSession, BaseContextProvider, SessionContext

logger = logging.getLogger(__name__)

# Cosmos DB MongoDB OIDC scope
_COSMOS_SCOPE = "https://cosmos.azure.com/.default"


class _AzureOIDCCallback(OIDCCallback):
    """Provides Azure Entra ID tokens to the pymongo OIDC auth flow."""

    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        credential = DefaultAzureCredential()
        token = credential.get_token(_COSMOS_SCOPE)
        return OIDCCallbackResult(access_token=token.token)


class CosmosSessionProvider(BaseContextProvider):
    """Loads / saves agent session state in Azure Cosmos DB MongoDB vCluster."""

    DEFAULT_SOURCE_ID = "cosmos_session"

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        database_name: str | None = None,
        collection_name: str | None = None,
    ):
        super().__init__(self.DEFAULT_SOURCE_ID)

        self._connection_string = (
            connection_string or os.environ["COSMOS_CONNECTION_STRING"]
        )
        self._database_name = database_name or os.getenv(
            "COSMOS_DATABASE", "agent_sessions"
        )
        self._collection_name = collection_name or os.getenv(
            "COSMOS_COLLECTION", os.getenv("COSMOS_CONTAINER", "sessions")
        )
        self._client: motor.motor_asyncio.AsyncIOMotorClient | None = None
        self._collection = None

    # ------------------------------------------------------------------
    # Lazy initialisation — reuse a single client (driver best practice)
    # ------------------------------------------------------------------
    async def _ensure_collection(self) -> None:
        if self._collection is not None:
            return

        self._client = motor.motor_asyncio.AsyncIOMotorClient(
            self._connection_string,
            authMechanismProperties={"OIDC_CALLBACK": _AzureOIDCCallback()},
        )
        db = self._client[self._database_name]
        self._collection = db[self._collection_name]

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------
    async def before_run(
        self,
        *,
        agent: Any,
        session: AgentSession,
        context: SessionContext,
        state: dict[str, Any],
    ) -> None:
        """Load persisted state from Cosmos DB into the session state dict."""
        await self._ensure_collection()

        session_id = session.session_id
        doc = await self._collection.find_one({"session_id": session_id})
        if doc:
            persisted_state: dict = doc.get("state", {})
            state.update(persisted_state)
            logger.info("Loaded session state from Cosmos DB: %s", session_id)
        else:
            logger.info("No existing session in Cosmos DB for %s — starting fresh", session_id)

        # Inject any personalisation we already know about
        user_name = state.get("user_name")
        if user_name:
            context.extend_instructions(
                self.source_id,
                f"The user's name is {user_name}. Always address them by name.",
            )

    async def after_run(
        self,
        *,
        agent: Any,
        session: AgentSession,
        context: SessionContext,
        state: dict[str, Any],
    ) -> None:
        """Persist the (possibly updated) session state back to Cosmos DB."""
        await self._ensure_collection()

        session_id = session.session_id
        now = datetime.now(timezone.utc).isoformat()

        # Extract user name from new messages if not already stored
        for msg in context.input_messages:
            text = msg.text if hasattr(msg, "text") else ""
            if isinstance(text, str) and "my name is" in text.lower():
                state["user_name"] = (
                    text.lower().split("my name is")[-1].strip().split()[0].capitalize()
                )

        state["last_active"] = now
        state["turn_count"] = state.get("turn_count", 0) + 1

        now_field = now
        update: dict[str, Any] = {
            "$set": {
                "session_id": session_id,
                "state": state,
                "agent_name": getattr(agent, "name", "unknown"),
                "updated_at": now_field,
            },
            "$setOnInsert": {
                "created_at": now_field,
            },
        }
        await self._collection.update_one(
            {"session_id": session_id}, update, upsert=True
        )
        logger.info("Persisted session state to Cosmos DB: %s (turn %d)", session_id, state["turn_count"])

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    async def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    async def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None
