---
description: "Azure portfolio review using MSA_AzureConsumption_Enterprise (MSXI). Answers: gap to target, pipeline conversion ranking, and recommended actions. Pulls ACR actuals, budget attainment, pipeline, and account attributes from Power BI with user-scoped filters."
---
# Azure Enterprise Portfolio Review

Analyze my Azure portfolio using the **MSA_AzureConsumption_Enterprise** Power BI model. Answer three questions:

1. **What is my gap to target?** — ACR actuals vs. budget, pipeline sufficiency
2. **Which opportunities have the highest chance to convert?** — Ranked by stage, commitment, and movement signals
3. **What can I do to close my gap?** — Actionable recommendations from account attributes and pipeline state

## Configuration

> **Managers**: Fork this file and update these values for your team's model and scope.

| Setting                       | Value                                                | Notes                                                                                            |
| ----------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| **Semantic Model ID**   | `{{SEMANTIC_MODEL_ID}}`             | MSA_AzureConsumption_Enterprise                                     |
| **Report ID**           | `{{REPORT_ID}}`             | [Open in Power BI]({{REPORT_URL}}) |
| **Account Roster**      | *(user-provided or `.docs/AccountReference.md`)* | Optional — if not set, prompt asks interactively                                                |
| **Default Date Filter** | `'DimDate'[IsAzureClosedAndCurrentOpen] = "Y"`     | YTD closed months + current open month                                                           |
| **Default View**        | `'DimViewType'[ViewType] = "Curated"`              | Standard curated view                                                                            |

## Workflow

### Step 0 — Power BI Auth Pre-Check

```dax
EVALUATE TOPN(1, 'DimDate')
```

If this returns data → auth is good, proceed.

If this fails → **stop** and tell the user:

> Power BI MCP authentication has expired. Please run:
>
> ```
> az login --tenant {{AZURE_TENANT_ID}}
> az account get-access-token --resource https://analysis.windows.net/powerbi/api
> ```
>
> Then restart `powerbi-remote` in VS Code (MCP icon → restart).

### Step 1 — Scope the Query

Before pulling any data, ask the user to narrow the scope:

> **To scope your portfolio review, tell me:**
>
> 1. **Which accounts?** (names, TPIDs, "all my assigned accounts", or an ATU/territory)
> 2. **Solution play or pillar filter?** (or "all")
> 3. **Time window?** (default: FY26 YTD — closed months + current open)

**Required — pick at least one:**

| Filter           | Column                                                                                          | Example               |
| ---------------- | ----------------------------------------------------------------------------------------------- | --------------------- |
| Customer name(s) | `'DimCustomer'[TPAccountName]`                                                                | "Contoso", "Fabrikam" |
| TPID(s)          | `'DimCustomer'[TPID]`                                                                         | 12345, 67890          |
| ATU / Territory  | `'DimAccountSummaryGroupingDscOCDM'[ATUName]` or `[ATUGroup]`                               | "US East Healthcare"  |
| Field Area       | `'DimAccountSummaryGroupingDscOCDM'[FieldArea]`                                               | "East"                |
| Role alias       | `'CustomerAssignByRole'[CSACloudAndAI]`, `[SpecialistCloudAndAI]`, `[SECloudAndAI]`, etc. | "jlee"                |

**Optional refinements:**

| Filter           | Column                                                                      | Example               |
| ---------------- | --------------------------------------------------------------------------- | --------------------- |
| Solution Play    | `'F_AzureConsumptionPipe'[SolutionPlay]`                                  | "Migrate & Modernize" |
| Strategic Pillar | `'F_AzureConsumptionPipe'[StrategicPillar]` or `[SuperStrategicPillar]` | "Infra", "Data & AI"  |
| Segment          | `'DimAccountSummaryGroupingDscOCDM'[Segment]`                             | "Enterprise"          |
| Fiscal period    | `'DimDate'[FY_Rel]` or `'DimDate'[Qtr_Rel]`                             | "FY", "CQ"            |
| Sales stage      | `'F_AzureConsumptionPipe'[SalesStageName]`                                | "Qualify", "Develop"  |

If user says **"all my assigned accounts"**:

- Use `msx-crm:crm_whoami` to get alias
- Query `CustomerAssignByRole` filtered by that alias across role columns to get TPID list

Build a `<SCOPE_FILTER>` from the user's choices. Always include these base filters:

```
'DimDate'[IsAzureClosedAndCurrentOpen] = "Y"
'DimViewType'[ViewType] = "Curated"
```

### Step 2 — Pull ACR Actuals + Budget (Gap to Target)

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
        <SCOPE_FILTER>
    ),
    [ACR_YTD], DESC
)
```

### Step 3 — Pull Pipeline Summary

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
        'DimDate'[FY_Rel] = "FY",
        'DimViewType'[ViewType] = "Curated",
        <ACCOUNT_SCOPE_FILTER>
    ),
    [Pipe_All], DESC
)
```

### Step 4 — Pull Opportunity-Level Detail

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
    <SCOPE_FILTER>,
    'F_AzureConsumptionPipe'[MilestoneStatus] IN {"In Progress", "Not Started", "Blocked"}
)
ORDER BY [SalesStage] DESC, [Account] ASC
```

### Step 5 — Pull Account Attributes (for recommendations)

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
    ),
    <ACCOUNT_SCOPE_FILTER>
)
```

### Step 6 — Digest & Bridge

> **Context management**: Steps 2–5 return raw tabular data that can consume 15–20K tokens for a 20-account portfolio. Before analysis, compress raw results into a compact digest. See `pbi-context-bridge.instructions.md` for the full schema.

Immediately after Steps 2–5 complete, build a digest with:

1. **`summary`** — aggregate ACR, pipeline, budget attainment, gap signal distribution (counts only).
2. **`accounts[]`** — one entry per account with `gap_signal`, key metrics, `flags[]`. Omit "ahead"/"on track" accounts with no flags.
3. **`ranked_opportunities[]`** — top 15 by conversion score (from Step 7 logic below). Include `oppty_number` and `crm_link`.
4. **`actions[]`** — each with a `downstream_query` hint for CRM/WorkIQ follow-up.

Write the digest to `.copilot/sessions/pbi/digest.md`. All subsequent analysis (Steps 7–10) works from the digest, not raw query results.

### Step 7 — Analyze: Gap to Target

Merge ACR + budget with pipeline data per account (from digest `accounts[]`):

| Account | TPID | ACR YTD | ACR LCM | Budget Attain% | Pipeline ($) | Committed Pipe ($) | Pipe WoW Δ | Milestones | Gap Signal |  |
| ------- | ---- | ------- | ------- | -------------- | ----------------------------------- | ----------- | ---------- | ---------- | - |

**Gap Signal logic:**

- **Ahead** — Budget attainment ≥ 100% YTD
- **On track** — Budget attainment 80–99% with positive pipeline and WoW growth
- **At risk** — Budget attainment < 80%, or pipeline declining WoW, or thin milestone coverage (< 3)
- **Needs pipeline** — ACR present but fewer than 2 active milestones
- **Stalled** — No WoW pipeline movement and no stage changes in Step 4 data

### Step 8 — Analyze: Conversion Ranking

Using digest `ranked_opportunities[]`, score and rank:

| Signal                        | Points | Source                                         |
| ----------------------------- | ------ | ---------------------------------------------- |
| Stage = "Close"               | +3     | `SalesStageName`                             |
| Stage = "Develop"             | +2     | `SalesStageName`                             |
| Commitment = "Committed"      | +3     | `CommitmentRecommendation`                   |
| Stage advanced this week      | +2     | `SalesStageCW1_Movement` shows advancement   |
| Commitment upgraded this week | +2     | `CommitmentChangeCW1` shows upgrade          |
| No risk/blockers              | +1     | `RiskBlockerDetails` is empty                |
| Completion month this quarter | +1     | `MilestoneCompletionMonth` within current FQ |
| Partner attached              | +1     | `IsOpptySharedWithPartner` = "Yes"           |

Present as:

| Rank | Opportunity | Account | Stage | Commitment | Score | Key Signal | CRM Link |
| ---- | ----------- | ------- | ----- | ---------- | ----- | ---------- | -------- |

### Step 9 — Analyze: Recommendations to Close Gap

For each account in digest with gap signal "at_risk", "needs_pipeline", or "stalled", apply rules using digest `flags[]` and `attributes`:

| Condition                             | Recommendation                                                                                          |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Uncommitted milestone past due        | "Milestone '{name}' is past completion date — update date or flip to committed. Confirm with {owner}." |
| Blocked milestone                     | "'{name}' blocked: {RiskBlockerDetails}. Escalate to unblock."                                          |
| No stage/commitment movement 2+ weeks | "'{opportunity}' stalled. Schedule next-step call."                                                     |
| < 2 active milestones                 | "Thin pipeline — check propensity flags for expansion plays."                                          |
| HasOpenAI = Y, no OpenAI pipe         | "Account uses OpenAI with no pipeline. Explore AI expansion."                                           |
| PTU_Target = Y, no PTU pipe           | "PTU target with no PTU pipeline. Engage SE for sizing."                                                |
| NetNewMigrationTarget = Y             | "Migration target. Check for workload modernization plays."                                             |
| 500K_100K_Target set                  | "High-value target — ensure coverage across solution areas."                                           |
| GHCP FY26 flags set                   | "GHCP incentive account — cross-reference with GHCP New Logo prompt for eligibility."                  |
| ESI activated, not certified          | "ESI activated but not certified. Skilling engagement may accelerate."                                  |
| Pipeline declining WoW                | "Pipeline dropped ${WoW_change} this week. Investigate lost/stalled milestones."                        |

### Step 10 — Present Final Report

**Section 1: Portfolio Summary**

- Accounts in scope: N
- Total ACR YTD: $X | Last closed month: $Y
- Budget attainment (aggregate): Z%
- Pipeline: $X total | $Y committed | WoW Δ: $Z
- Gap signals: N ahead | N on track | N at risk | N needs pipeline | N stalled

**Section 2: Gap Analysis** (table from Step 6)

**Section 3: Top Conversion Opportunities** (table from Step 7, top 10)

**Section 4: Recommended Actions** (from Step 8, ordered by impact)

- Number each action with CRM link
- Tag: 🏃 Quick win (< 5 min fix) vs. 📋 Strategic action

**Section 5: Scope & Data Freshness**

- Model: MSA_AzureConsumption_Enterprise (`{{SEMANTIC_MODEL_ID}}`)
- Filters applied: list user's scope selections
- Date context: report the `DimDate[Wk_Rel]` and `DimDate[Month_Rel]` from latest data
- Note: "Budget measures reflect org-level targets. Account-level attainment is derived from the `% ACR Budget Attain (YTD)` measure."

### Downstream Operations (Optional)

With the digest persisted to `.copilot/sessions/pbi/digest.md`, you can chain into these operations without re-running PBI queries:

- **"Cross-check at-risk accounts in CRM"** → reads digest `accounts[]` where `gap_signal` is `at_risk`/`stalled`, runs `msx-crm:get_milestones` for those TPIDs to get live milestone state
- **"Check WorkIQ for stalled accounts"** → reads digest `actions[].downstream_query` hints, scopes `workiq:ask_work_iq` to flagged accounts + last 14 days
- **"Run pipeline hygiene triage"** → digest provides the prioritized account + opportunity list for `pipeline-hygiene-triage` skill
- **"Full deal triage on worst accounts"** → take top 3 `at_risk` accounts from digest, run `mcem-stage-identification` → `exit-criteria-validation` → `risk-surfacing`

See `pbi-context-bridge.instructions.md` and `shared-patterns.instructions.md` § Common multi-skill chains for the full composition patterns.
