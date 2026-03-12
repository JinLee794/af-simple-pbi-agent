"""Pre-validated DAX query catalog for MSA_AzureConsumption_Enterprise.

Contains battle-tested DAX queries keyed by intent. The agent looks up
a query by intent name, gets back ready-to-execute DAX with scope filters
already injected. Zero DAX hallucination risk — these are the fast path.
"""

import os
from typing import Annotated

from tools._filters import (
    SEMANTIC_MODEL_ID,
    BASE_ACR_FILTERS,
    BASE_PIPELINE_FILTERS,
    build_filters,
    parse_csv,
    escape_dax,
)

TENANT_ID = os.getenv("AZURE_TENANT_ID", "<your-tenant-id>")

# ── Query Templates ──────────────────────────────────────────────────────────
# Each entry: (builder_function, description shown to agent)
# Builder functions accept (acr_scope, pipe_scope, account_scope) strings.

_CATALOG: dict[str, tuple[str, str]] = {}


def _register(intent: str, description: str):
    """Decorator to register a query builder in the catalog."""
    def decorator(fn):
        _CATALOG[intent] = (description, fn)
        return fn
    return decorator


# ── auth_check ───────────────────────────────────────────────────────────────
@_register(
    "auth_check",
    "Validates Power BI MCP authentication. Run this first before any data queries.",
)
def _auth_check(**_) -> str:
    return f"""## Auth Check
**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`

### Query
```dax
EVALUATE TOPN(1, 'DimDate')
```

If this returns data, auth is good — proceed with data queries.
If it fails, tell the user:
> Power BI MCP authentication has expired. Run:
> `az login --tenant {TENANT_ID}`
> `az account get-access-token --resource https://analysis.windows.net/powerbi/api`
> Then restart `powerbi-remote` in VS Code (MCP icon → restart)."""


# ── gap_to_target ────────────────────────────────────────────────────────────
@_register(
    "gap_to_target",
    "ACR actuals vs. budget + pipeline summary. Answers: 'What is my gap to target?'",
)
def _gap_to_target(acr_scope: str, pipe_scope: str, **_) -> str:
    return f"""## Gap-to-Target Queries
**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`

### Query 1 — ACR Actuals + Budget Attainment
```dax
EVALUATE
TOPN(
    100,
    CALCULATETABLE(
        SUMMARIZECOLUMNS(
            'DimCustomer'[TPAccountName],
            'DimCustomer'[TPID],
            "ACR_YTD", 'M_ACR'[$ ACR],
            "ACR_LCM", 'M_ACR'[$ ACR Last Closed Month],
            "Budget_Attain_YTD", 'M_ACR'[% ACR Budget Attain (YTD)]
        ),
        {acr_scope}
    ),
    [ACR_YTD], DESC
)
```

### Query 2 — Pipeline Summary
```dax
EVALUATE
TOPN(
    100,
    CALCULATETABLE(
        SUMMARIZECOLUMNS(
            'DimCustomer'[TPAccountName],
            'DimCustomer'[TPID],
            "Pipe_All", 'M_ACRPipe'[$ Consumption Pipeline All],
            "Pipe_WoW_Change", 'M_ACRPipe'[$ Consumption Pipeline All WoW Change],
            "Qualified_Pipe", 'M_ACRPipe'[$ Qualified Pipeline Prior Week all],
            "Committed_Pipe", 'M_ACRPipe'[$ Consumption Committed Pipeline Prior Week All],
            "Milestones", 'M_ACRPipe'[# Milestones]
        ),
        {pipe_scope}
    ),
    [Pipe_All], DESC
)
```

### Gap Signal Classification
Merge both results by TPID and classify:

| Gap Signal     | Criteria |
|----------------|----------|
| Ahead          | Budget attainment ≥ 100% |
| On track       | 80–99% attainment with positive pipeline AND WoW growth |
| At risk        | < 80% attainment, OR pipeline declining WoW, OR < 3 milestones |
| Needs pipeline | ACR present but < 2 active milestones |
| Stalled        | No WoW pipeline movement and no stage changes |

Present as: Account | TPID | ACR YTD | ACR LCM | Budget Attain% | Pipeline ($) | Committed ($) | Pipe WoW Δ | Milestones | Gap Signal"""


# ── opportunity_conversion ───────────────────────────────────────────────────
@_register(
    "opportunity_conversion",
    "Opportunity-level detail + scoring rubric. Answers: 'Which opportunities will convert?'",
)
def _opportunity_conversion(acr_scope: str, **_) -> str:
    return f"""## Opportunity Conversion Query
**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`

### Query — Opportunity-Level Detail
```dax
EVALUATE
CALCULATETABLE(
    SELECTCOLUMNS(
        'F_AzureConsumptionPipe',
        "TPID", 'F_AzureConsumptionPipe'[TPID],
        "Account", 'F_AzureConsumptionPipe'[CRMAccountName],
        "Opportunity", 'F_AzureConsumptionPipe'[OpportunityName],
        "OpptyNumber", 'F_AzureConsumptionPipe'[OpportunityNumber],
        "SalesStage", 'F_AzureConsumptionPipe'[SalesStageName],
        "Commitment", 'F_AzureConsumptionPipe'[CommitmentRecommendation],
        "MilestoneStatus", 'F_AzureConsumptionPipe'[MilestoneStatus],
        "MilestoneName", 'F_AzureConsumptionPipe'[MilestoneName],
        "MilestoneOwner", 'F_AzureConsumptionPipe'[MilestoneOwner],
        "CompletionMonth", 'F_AzureConsumptionPipe'[MilestoneCompletionMonth],
        "SolutionPlay", 'F_AzureConsumptionPipe'[SolutionPlay],
        "Pillar", 'F_AzureConsumptionPipe'[StrategicPillar],
        "StageMove_WoW", 'F_AzureConsumptionPipe'[SalesStageCW1_Movement],
        "CommitChange_WoW", 'F_AzureConsumptionPipe'[CommitmentChangeCW1],
        "RiskBlockers", 'F_AzureConsumptionPipe'[RiskBlockerDetails],
        "CRMLink", 'F_AzureConsumptionPipe'[CRMLink],
        "PartnerShared", 'F_AzureConsumptionPipe'[IsOpptySharedWithPartner]
    ),
    {acr_scope},
    'F_AzureConsumptionPipe'[MilestoneStatus] IN {{"In Progress", "Not Started", "Blocked"}}
)
ORDER BY [SalesStage] DESC, [Account] ASC
```

### Conversion Scoring Rubric
Score each opportunity by summing applicable points:

| Signal                        | Points | Source Column                 |
|-------------------------------|--------|------------------------------|
| Stage = "Close"               | +3     | SalesStage                   |
| Stage = "Develop"             | +2     | SalesStage                   |
| Commitment = "Committed"      | +3     | Commitment                   |
| Stage advanced this week      | +2     | StageMove_WoW (advancement)  |
| Commitment upgraded this week | +2     | CommitChange_WoW (upgrade)   |
| No risk/blockers              | +1     | RiskBlockers is empty        |
| Completion month this quarter | +1     | CompletionMonth within current FQ |
| Partner attached              | +1     | PartnerShared = "Yes"        |

Present top 10 as: Rank | Opportunity | Account | Stage | Commitment | Score | Key Signal | CRM Link"""


# ── account_recommendations ─────────────────────────────────────────────────
@_register(
    "account_recommendations",
    "Account propensity flags + recommendation rules. Answers: 'What can I do to close my gap?'",
)
def _account_recommendations(account_scope: str, **_) -> str:
    if account_scope:
        filter_block = f""",
        {account_scope}"""
    else:
        filter_block = ""

    return f"""## Account Recommendations Query
**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`

### Query — Account Attributes
```dax
EVALUATE
CALCULATETABLE(
    SELECTCOLUMNS(
        'AzureCustomerAttributes',
        "TPID", 'AzureCustomerAttributes'[TPID],
        "ESI_Tier", 'AzureCustomerAttributes'[ESI_Tier],
        "HasOpenAI", 'AzureCustomerAttributes'[HasOpenAI],
        "HasOpenAI_Pipe", 'AzureCustomerAttributes'[HasOpenAI_Pipe],
        "PTU_Target", 'AzureCustomerAttributes'[PTU_Target_Customer],
        "500K_100K_Target", 'AzureCustomerAttributes'[500K_100K_Targets],
        "NetNewMigrationTarget", 'AzureCustomerAttributes'[NetNewMigrationTarget],
        "LXP_Category", 'AzureCustomerAttributes'[LXP_Category],
        "TrancheGrowthTarget", 'AzureCustomerAttributes'[TrancheGrowthTargetAccounts],
        "GHCP_200Plus", 'AzureCustomerAttributes'[GHCPFY26200Plus],
        "GHCP_200Less", 'AzureCustomerAttributes'[GHCPFY26200Less]
    ){filter_block}
)
```

### Recommendation Rules
For each account flagged as "at risk", "needs pipeline", or "stalled" in gap analysis:

| Condition                                | Recommendation | Tag |
|------------------------------------------|----------------|-----|
| Uncommitted milestone past due           | Update date or flip to committed. Confirm with owner. | 🏃 Quick win |
| Blocked milestone                        | Escalate to unblock. | 🏃 Quick win |
| No stage/commitment movement 2+ weeks    | Schedule next-step call. | 📋 Strategic |
| < 2 active milestones                    | Check propensity flags for expansion plays. | 📋 Strategic |
| HasOpenAI = Y but no OpenAI pipe         | Explore AI expansion opportunity. | 📋 Strategic |
| PTU_Target = Y but no PTU pipe           | Engage SE for PTU sizing. | 📋 Strategic |
| NetNewMigrationTarget = Y                | Check for workload modernization plays. | 📋 Strategic |
| 500K_100K_Target set                     | Ensure coverage across solution areas. | 📋 Strategic |
| GHCP FY26 flags set                      | Cross-reference with GHCP New Logo prompt. | 📋 Strategic |
| ESI activated but not certified          | Skilling engagement may accelerate. | 📋 Strategic |
| Pipeline declining WoW                   | Investigate lost/stalled milestones. | 🏃 Quick win |"""


# ── Public tool function ─────────────────────────────────────────────────────
def lookup_prevalidated_dax(
    intent: Annotated[
        str,
        "Query intent. One of: 'auth_check', 'gap_to_target', 'opportunity_conversion', 'account_recommendations'. "
        "Call with 'auth_check' first to verify Power BI connectivity.",
    ],
    tpids: Annotated[str, "Comma-separated TPIDs, e.g. '12345, 67890'."] = "",
    account_names: Annotated[str, "Comma-separated account names (TPAccountName), e.g. 'Contoso, Fabrikam'."] = "",
    solution_play: Annotated[str, "Optional solution play filter, e.g. 'Migrate & Modernize'."] = "",
    strategic_pillar: Annotated[str, "Optional strategic pillar filter, e.g. 'Infra', 'Data & AI'."] = "",
) -> str:
    """Look up a pre-validated DAX query by intent, with scope filters injected.

    Returns ready-to-execute DAX queries plus analysis guidance. Execute the
    returned DAX via Power BI MCP (ExecuteQuery) using the semantic model ID
    included in the response.

    Available intents:
    - auth_check: Verify PBI auth (no scope params needed)
    - gap_to_target: ACR actuals + budget + pipeline (2 queries)
    - opportunity_conversion: Opportunity detail + scoring rubric
    - account_recommendations: Account attributes + recommendation rules

    If your question is NOT covered by these intents, use get_semantic_model_schema
    to discover available tables/columns, then build_custom_dax_query to assemble
    a valid query.
    """
    intent_key = intent.strip().lower().replace(" ", "_").replace("-", "_")

    if intent_key not in _CATALOG:
        available = "\n".join(
            f"  - `{k}`: {desc}" for k, (desc, _) in _CATALOG.items()
        )
        return (
            f"Unknown intent: '{intent}'. Available pre-validated queries:\n{available}\n\n"
            "If none of these match, use `get_semantic_model_schema` to explore the model, "
            "then `build_custom_dax_query` to assemble a custom query."
        )

    _, builder = _CATALOG[intent_key]

    # Build scoped filters
    acr_scope = build_filters(tpids, account_names, solution_play, strategic_pillar, BASE_ACR_FILTERS)
    pipe_scope = build_filters(tpids, account_names, solution_play, strategic_pillar, BASE_PIPELINE_FILTERS)

    # Account-only scope (no date/view base filters) for attributes table
    account_filters: list[str] = []
    parsed_tpids = parse_csv(tpids)
    if parsed_tpids:
        account_filters.append(f"'AzureCustomerAttributes'[TPID] IN {{{', '.join(parsed_tpids)}}}")
    parsed_names = parse_csv(account_names)
    if parsed_names:
        escaped = [f'"{escape_dax(n)}"' for n in parsed_names]
        account_filters.append(
            f"'AzureCustomerAttributes'[TPID] IN "
            f"(SELECTCOLUMNS(FILTER('DimCustomer', "
            f"'DimCustomer'[TPAccountName] IN {{{', '.join(escaped)}}}), "
            f"\"TPID\", 'DimCustomer'[TPID]))"
        )
    account_scope = ",\n        ".join(account_filters)

    return builder(acr_scope=acr_scope, pipe_scope=pipe_scope, account_scope=account_scope)
