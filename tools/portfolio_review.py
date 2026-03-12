"""Azure Portfolio Review Tools — DAX query generators for MSA_AzureConsumption_Enterprise.

Three tools mapping to the three core portfolio review questions:
  1. get_gap_to_target_queries        — "What is my gap to target?"
  2. get_opportunity_conversion_query  — "Which opportunities will convert?"
  3. get_account_recommendations_query — "What should I do to close the gap?"

All tools accept the same scope parameters and return ready-to-execute DAX queries
with embedded analysis guidance. Execute queries via Power BI MCP (ExecuteQuery).
"""

from typing import Annotated

from tools._filters import (
    SEMANTIC_MODEL_ID,
    REPORT_ID,
    BASE_ACR_FILTERS as _BASE_ACR_FILTERS,
    BASE_PIPELINE_FILTERS as _BASE_PIPELINE_FILTERS,
    escape_dax as _escape_dax,
    parse_csv as _parse_csv,
    build_filters as _build_filters,
)


# ── Tool 1: Gap to Target ───────────────────────────────────────────────────
def get_gap_to_target_queries(
    tpids: Annotated[str, "Comma-separated TPIDs, e.g. '12345, 67890'. Provide tpids and/or account_names."] = "",
    account_names: Annotated[str, "Comma-separated account names (TPAccountName), e.g. 'Contoso, Fabrikam'."] = "",
    solution_play: Annotated[str, "Optional solution play filter, e.g. 'Migrate & Modernize'."] = "",
    strategic_pillar: Annotated[str, "Optional strategic pillar filter, e.g. 'Infra', 'Data & AI'."] = "",
) -> str:
    """Generate DAX queries for gap-to-target analysis: ACR actuals, budget attainment, and pipeline summary.

    Returns two ready-to-execute DAX queries against MSA_AzureConsumption_Enterprise
    plus gap-signal classification rules. Execute both via Power BI MCP (ExecuteQuery),
    then merge results by TPID to assess each account's gap to target.

    Use this tool to answer: "What is my gap to target?"
    """
    acr_scope = _build_filters(tpids, account_names, solution_play, strategic_pillar, _BASE_ACR_FILTERS)
    pipe_scope = _build_filters(tpids, account_names, solution_play, strategic_pillar, _BASE_PIPELINE_FILTERS)

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
After executing both queries, merge results by TPID and classify each account:

| Gap Signal     | Criteria |
|----------------|----------|
| Ahead          | Budget attainment ≥ 100% |
| On track       | 80–99% attainment with positive pipeline AND WoW growth |
| At risk        | < 80% attainment, OR pipeline declining WoW, OR < 3 milestones |
| Needs pipeline | ACR present but < 2 active milestones |
| Stalled        | No WoW pipeline movement and no stage changes |

Present as: Account | TPID | ACR YTD | ACR LCM | Budget Attain% | Pipeline ($) | Committed ($) | Pipe WoW Δ | Milestones | Gap Signal"""


# ── Tool 2: Opportunity Conversion Ranking ───────────────────────────────────
def get_opportunity_conversion_query(
    tpids: Annotated[str, "Comma-separated TPIDs, e.g. '12345, 67890'. Provide tpids and/or account_names."] = "",
    account_names: Annotated[str, "Comma-separated account names (TPAccountName), e.g. 'Contoso, Fabrikam'."] = "",
    solution_play: Annotated[str, "Optional solution play filter, e.g. 'Migrate & Modernize'."] = "",
    strategic_pillar: Annotated[str, "Optional strategic pillar filter, e.g. 'Infra', 'Data & AI'."] = "",
) -> str:
    """Generate a DAX query for opportunity-level detail with a conversion scoring rubric.

    Returns a ready-to-execute DAX query that pulls all active opportunities
    (In Progress, Not Started, Blocked milestones) with stage, commitment,
    movement signals, risk/blockers, and CRM links. Includes a point-based
    scoring rubric to rank opportunities by likelihood of conversion.

    Use this tool to answer: "Which opportunities have the highest chance to convert?"
    """
    scope = _build_filters(tpids, account_names, solution_play, strategic_pillar, _BASE_ACR_FILTERS)

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
    {scope},
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


# ── Tool 3: Account Attributes & Recommendations ────────────────────────────
def get_account_recommendations_query(
    tpids: Annotated[str, "Comma-separated TPIDs, e.g. '12345, 67890'. Provide tpids and/or account_names."] = "",
    account_names: Annotated[str, "Comma-separated account names (TPAccountName), e.g. 'Contoso, Fabrikam'."] = "",
    solution_play: Annotated[str, "Optional solution play filter, e.g. 'Migrate & Modernize'."] = "",
    strategic_pillar: Annotated[str, "Optional strategic pillar filter, e.g. 'Infra', 'Data & AI'."] = "",
) -> str:
    """Generate a DAX query for account attributes with a recommendation rules matrix.

    Returns a ready-to-execute DAX query that pulls propensity flags, targets,
    and program eligibility per account from AzureCustomerAttributes. Includes
    a condition-to-recommendation mapping to generate actionable next steps
    for accounts that are at risk, need pipeline, or are stalled.

    Use this tool to answer: "What can I do to close my gap?"
    Pair with gap-to-target results to focus recommendations on flagged accounts.
    """
    # Account attributes only need TPID scope, not date/view filters
    filters: list[str] = []
    parsed_tpids = _parse_csv(tpids)
    if parsed_tpids:
        filters.append(
            f"'AzureCustomerAttributes'[TPID] IN {{{', '.join(parsed_tpids)}}}"
        )
    parsed_names = _parse_csv(account_names)
    if parsed_names:
        escaped = [f'"{_escape_dax(n)}"' for n in parsed_names]
        filters.append(
            f"'AzureCustomerAttributes'[TPID] IN "
            f"(SELECTCOLUMNS(FILTER('DimCustomer', "
            f"'DimCustomer'[TPAccountName] IN {{{', '.join(escaped)}}}), "
            f"\"TPID\", 'DimCustomer'[TPID]))"
        )

    # Fall back to unfiltered if no scope provided
    filter_clause = ",\n        ".join(filters) if filters else ""
    calculate_wrap = f"""CALCULATETABLE(
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
    ){f''',
        {filter_clause}''' if filter_clause else ''}
)"""

    return f"""## Account Recommendations Query
**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`

### Query — Account Attributes
```dax
EVALUATE
{calculate_wrap}
```

### Recommendation Rules
For each account flagged as "at risk", "needs pipeline", or "stalled" in gap analysis, apply these rules:

| Condition                                | Recommendation |
|------------------------------------------|----------------|
| Uncommitted milestone past due           | "Milestone '{{name}}' is past completion date — update date or flip to committed. Confirm with {{owner}}." |
| Blocked milestone                        | "'{{name}}' blocked: {{RiskBlockerDetails}}. Escalate to unblock." |
| No stage/commitment movement 2+ weeks    | "'{{opportunity}}' stalled. Schedule next-step call." |
| < 2 active milestones                    | "Thin pipeline — check propensity flags for expansion plays." |
| HasOpenAI = Y but no OpenAI pipe         | "Account uses OpenAI with no pipeline. Explore AI expansion." |
| PTU_Target = Y but no PTU pipe           | "PTU target with no PTU pipeline. Engage SE for sizing." |
| NetNewMigrationTarget = Y                | "Migration target. Check for workload modernization plays." |
| 500K_100K_Target set                     | "High-value target — ensure coverage across solution areas." |
| GHCP FY26 flags set                      | "GHCP incentive account — cross-reference with GHCP New Logo prompt for eligibility." |
| ESI activated but not certified          | "ESI activated but not certified. Skilling engagement may accelerate." |
| Pipeline declining WoW                   | "Pipeline dropped ${{WoW_change}} this week. Investigate lost/stalled milestones." |

Tag each recommendation: 🏃 Quick win (< 5 min) or 📋 Strategic action.
Include CRM links from opportunity data where applicable."""
