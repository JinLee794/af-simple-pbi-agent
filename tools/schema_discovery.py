"""Semantic model schema discovery for MSA_AzureConsumption_Enterprise.

Returns the full known schema — tables, columns (with types and descriptions),
measures, and relationships. The agent uses this to understand what data is
available when no pre-validated DAX query covers the user's question.
"""

from typing import Annotated

from tools._filters import SEMANTIC_MODEL_ID

# ── Known Schema ─────────────────────────────────────────────────────────────
# Hardcoded from the documented model. This is faster than a live MCP call and
# gives the agent everything it needs to write correct column references.

_TABLES: dict[str, dict] = {
    "DimCustomer": {
        "description": "Customer dimension — account names, TPIDs, and org hierarchy.",
        "columns": {
            "TPAccountName": {"type": "String", "desc": "Top-parent account name"},
            "TPID": {"type": "Integer", "desc": "Top-parent account ID (primary key for joins)"},
        },
    },
    "DimDate": {
        "description": "Date/fiscal-period dimension with relative period flags.",
        "columns": {
            "IsAzureClosedAndCurrentOpen": {"type": "String", "desc": "Y/N — YTD closed months + current open"},
            "FY_Rel": {"type": "String", "desc": "Relative fiscal year, e.g. 'FY' = current FY"},
            "Qtr_Rel": {"type": "String", "desc": "Relative quarter, e.g. 'CQ' = current quarter"},
            "Wk_Rel": {"type": "String", "desc": "Relative week label"},
            "Month_Rel": {"type": "String", "desc": "Relative month label"},
        },
    },
    "DimViewType": {
        "description": "View-type slicer (Curated vs Raw).",
        "columns": {
            "ViewType": {"type": "String", "desc": "'Curated' or 'Raw'"},
        },
    },
    "DimAccountSummaryGroupingDscOCDM": {
        "description": "Account org-mapping — ATU, field area, segment.",
        "columns": {
            "ATUName": {"type": "String", "desc": "ATU name"},
            "ATUGroup": {"type": "String", "desc": "ATU group"},
            "FieldArea": {"type": "String", "desc": "Field area (e.g. 'East')"},
            "Segment": {"type": "String", "desc": "Segment (e.g. 'Enterprise')"},
        },
    },
    "CustomerAssignByRole": {
        "description": "Role-based account assignments — who owns what.",
        "columns": {
            "CSACloudAndAI": {"type": "String", "desc": "CSA alias"},
            "SpecialistCloudAndAI": {"type": "String", "desc": "Specialist alias"},
            "SECloudAndAI": {"type": "String", "desc": "SE alias"},
        },
    },
    "F_AzureConsumptionPipe": {
        "description": "Fact table — pipeline opportunities, milestones, stages, commitments.",
        "columns": {
            "TPID": {"type": "Integer", "desc": "Account TPID (FK to DimCustomer)"},
            "CRMAccountName": {"type": "String", "desc": "CRM account name"},
            "OpportunityName": {"type": "String", "desc": "Opportunity title"},
            "OpportunityNumber": {"type": "String", "desc": "CRM opportunity number"},
            "SalesStageName": {"type": "String", "desc": "Current sales stage (Qualify, Develop, Close, etc.)"},
            "CommitmentRecommendation": {"type": "String", "desc": "Committed / Uncommitted / etc."},
            "MilestoneStatus": {"type": "String", "desc": "In Progress, Not Started, Blocked, Completed"},
            "MilestoneName": {"type": "String", "desc": "Milestone display name"},
            "MilestoneOwner": {"type": "String", "desc": "Owner alias"},
            "MilestoneCompletionMonth": {"type": "String", "desc": "Expected completion month"},
            "SolutionPlay": {"type": "String", "desc": "e.g. Migrate & Modernize, Innovate"},
            "StrategicPillar": {"type": "String", "desc": "e.g. Infra, Data & AI"},
            "SuperStrategicPillar": {"type": "String", "desc": "Super pillar grouping"},
            "SalesStageCW1_Movement": {"type": "String", "desc": "Stage movement vs. prior week"},
            "CommitmentChangeCW1": {"type": "String", "desc": "Commitment change vs. prior week"},
            "RiskBlockerDetails": {"type": "String", "desc": "Free-text risk/blocker info"},
            "CRMLink": {"type": "String", "desc": "URL to CRM opportunity"},
            "IsOpptySharedWithPartner": {"type": "String", "desc": "Yes/No partner sharing flag"},
        },
    },
    "AzureCustomerAttributes": {
        "description": "Account propensity/target flags for recommendations.",
        "columns": {
            "TPID": {"type": "Integer", "desc": "Account TPID"},
            "ESI_Tier": {"type": "String", "desc": "ESI tier level"},
            "HasOpenAI": {"type": "String", "desc": "Y/N — account uses OpenAI"},
            "HasOpenAI_Pipe": {"type": "String", "desc": "Y/N — has OpenAI pipeline"},
            "PTU_Target_Customer": {"type": "String", "desc": "Y/N — PTU target"},
            "500K_100K_Targets": {"type": "String", "desc": "High-value account target flag"},
            "NetNewMigrationTarget": {"type": "String", "desc": "Y/N — migration target"},
            "LXP_Category": {"type": "String", "desc": "Landing/expansion category"},
            "TrancheGrowthTargetAccounts": {"type": "String", "desc": "Y/N — tranche growth target"},
            "GHCPFY26200Plus": {"type": "String", "desc": "GHCP FY26 200+ flag"},
            "GHCPFY26200Less": {"type": "String", "desc": "GHCP FY26 <200 flag"},
        },
    },
}

_MEASURES: dict[str, dict[str, str]] = {
    "M_ACR": {
        "$ ACR": "ACR total (YTD by default with date filter)",
        "$ ACR Last Closed Month": "ACR for the most recent closed month",
        "% ACR Budget Attain (YTD)": "Budget attainment percentage YTD",
    },
    "M_ACRPipe": {
        "$ Consumption Pipeline All": "Total pipeline value",
        "$ Consumption Pipeline All WoW Change": "Week-over-week pipeline change",
        "$ Qualified Pipeline Prior Week all": "Qualified pipeline (prior week)",
        "$ Consumption Committed Pipeline Prior Week All": "Committed pipeline (prior week)",
        "# Milestones": "Count of milestones",
    },
}

_RELATIONSHIPS = [
    "DimCustomer[TPID] → F_AzureConsumptionPipe[TPID]",
    "DimCustomer[TPID] → AzureCustomerAttributes[TPID]",
    "DimDate filters F_AzureConsumptionPipe (date context)",
    "DimViewType filters all fact tables (Curated/Raw slicer)",
    "DimAccountSummaryGroupingDscOCDM joins to DimCustomer (org hierarchy)",
    "CustomerAssignByRole joins to DimCustomer (role assignments)",
]


def get_semantic_model_schema(
    tables: Annotated[
        str,
        "Comma-separated table names to return (e.g. 'DimCustomer, F_AzureConsumptionPipe'). "
        "Leave empty to return the full model schema.",
    ] = "",
) -> str:
    """Return the known schema of MSA_AzureConsumption_Enterprise.

    Use this tool when no pre-validated DAX query (from lookup_prevalidated_dax)
    covers the user's question. Returns tables, columns with data types and
    descriptions, measures, and relationships so you can craft a custom query
    using build_custom_dax_query.

    The schema is derived from the documented model. For live discovery of
    unlisted tables, you can also call Power BI MCP GetSemanticModelSchema
    with model ID included in the response.
    """
    requested = {t.strip() for t in tables.split(",") if t.strip()} if tables.strip() else None

    lines = [
        f"## Semantic Model Schema — MSA_AzureConsumption_Enterprise",
        f"**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`",
        "",
    ]

    # Tables & columns
    lines.append("### Tables & Columns")
    for tbl_name, tbl_info in _TABLES.items():
        if requested and tbl_name not in requested:
            continue
        lines.append(f"\n**`{tbl_name}`** — {tbl_info['description']}")
        lines.append("| Column | Type | Description |")
        lines.append("|--------|------|-------------|")
        for col_name, col_info in tbl_info["columns"].items():
            lines.append(f"| `{col_name}` | {col_info['type']} | {col_info['desc']} |")

    # Measures
    if not requested or requested & set(_MEASURES.keys()):
        lines.append("\n### Measures")
        for group, measures in _MEASURES.items():
            if requested and group not in requested:
                continue
            lines.append(f"\n**`{group}`**")
            lines.append("| Measure | Description |")
            lines.append("|---------|-------------|")
            for m_name, m_desc in measures.items():
                lines.append(f"| `{m_name}` | {m_desc} |")

    # Relationships
    if not requested:
        lines.append("\n### Relationships")
        for rel in _RELATIONSHIPS:
            lines.append(f"- {rel}")

    # Guidance
    lines.append("\n### DAX Authoring Notes")
    lines.append("- Always wrap table names in single quotes: `'TableName'`")
    lines.append("- Always include base filters: `'DimDate'[IsAzureClosedAndCurrentOpen] = \"Y\"` and `'DimViewType'[ViewType] = \"Curated\"`")
    lines.append("- Use `CALCULATETABLE` with `SUMMARIZECOLUMNS` for aggregations")
    lines.append("- Use `SELECTCOLUMNS` for row-level detail from fact tables")
    lines.append("- Use `TOPN(100, ...)` to limit result sets")
    lines.append("- Join on `TPID` between tables")
    lines.append(f"- For live schema discovery beyond this list, call Power BI MCP `GetSemanticModelSchema` with model ID `{SEMANTIC_MODEL_ID}`")

    return "\n".join(lines)
