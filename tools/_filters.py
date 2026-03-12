"""Shared filter construction and DAX escaping for portfolio review tools."""

import os

SEMANTIC_MODEL_ID = os.environ["SEMANTIC_MODEL_ID"]
REPORT_ID = os.environ["REPORT_ID"]

BASE_ACR_FILTERS = [
    "'DimDate'[IsAzureClosedAndCurrentOpen] = \"Y\"",
    "'DimViewType'[ViewType] = \"Curated\"",
]
BASE_PIPELINE_FILTERS = [
    "'DimDate'[FY_Rel] = \"FY\"",
    "'DimViewType'[ViewType] = \"Curated\"",
]


def escape_dax(value: str) -> str:
    """Escape a value for safe use inside a DAX double-quoted string literal."""
    return value.replace('"', '""')


def parse_csv(raw: str) -> list[str]:
    """Split a comma-separated string into trimmed, non-empty tokens."""
    return [tok.strip() for tok in raw.split(",") if tok.strip()]


def build_filters(
    tpids: str,
    account_names: str,
    solution_play: str,
    strategic_pillar: str,
    base: list[str],
) -> str:
    """Assemble a comma-separated DAX filter clause from scope parameters."""
    filters = list(base)

    parsed_tpids = parse_csv(tpids)
    if parsed_tpids:
        filters.append(
            f"'DimCustomer'[TPID] IN {{{', '.join(parsed_tpids)}}}"
        )

    parsed_names = parse_csv(account_names)
    if parsed_names:
        escaped = [f'"{escape_dax(n)}"' for n in parsed_names]
        filters.append(
            f"'DimCustomer'[TPAccountName] IN {{{', '.join(escaped)}}}"
        )

    if solution_play.strip():
        filters.append(
            f"'F_AzureConsumptionPipe'[SolutionPlay] = \"{escape_dax(solution_play.strip())}\""
        )

    if strategic_pillar.strip():
        filters.append(
            f"'F_AzureConsumptionPipe'[StrategicPillar] = \"{escape_dax(strategic_pillar.strip())}\""
        )

    return ",\n        ".join(filters)
