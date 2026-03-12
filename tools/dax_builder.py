"""Structured DAX query builder for MSA_AzureConsumption_Enterprise.

Assembles syntactically valid DAX from structured inputs. The agent uses
get_semantic_model_schema to discover available tables/columns/measures,
then passes them here to produce a valid query — no free-form DAX writing.
"""

from typing import Annotated

from tools._filters import SEMANTIC_MODEL_ID, escape_dax, parse_csv


def build_custom_dax_query(
    table: Annotated[str, "Primary table name, e.g. 'F_AzureConsumptionPipe' or 'DimCustomer'."],
    columns: Annotated[
        str,
        "Comma-separated 'Table[Column]' references to include, "
        "e.g. \"'DimCustomer'[TPAccountName], 'DimCustomer'[TPID]\". "
        "For SUMMARIZECOLUMNS grouping columns.",
    ] = "",
    measures: Annotated[
        str,
        "Comma-separated measure expressions as 'Alias=MeasureRef', "
        "e.g. \"ACR_YTD='M_ACR'[$ ACR], Pipe='M_ACRPipe'[$ Consumption Pipeline All]\". "
        "Used as virtual columns in SUMMARIZECOLUMNS.",
    ] = "",
    select_columns: Annotated[
        str,
        "Comma-separated 'Alias=Table[Column]' for SELECTCOLUMNS (row-level detail, no aggregation), "
        "e.g. \"Account='F_AzureConsumptionPipe'[CRMAccountName], Stage='F_AzureConsumptionPipe'[SalesStageName]\". "
        "If provided, SELECTCOLUMNS is used instead of SUMMARIZECOLUMNS.",
    ] = "",
    filters: Annotated[
        str,
        "Comma-separated DAX filter expressions, "
        "e.g. \"'DimDate'[IsAzureClosedAndCurrentOpen] = \\\"Y\\\", 'DimViewType'[ViewType] = \\\"Curated\\\"\". "
        "Base date/view filters are NOT auto-added — include them if needed.",
    ] = "",
    top_n: Annotated[int, "Limit results with TOPN. 0 = no limit."] = 100,
    order_by: Annotated[str, "ORDER BY expression, e.g. '[ACR_YTD] DESC'. Leave empty for default ordering."] = "",
) -> str:
    """Build a syntactically valid DAX query from structured inputs.

    Use this tool when no pre-validated query (from lookup_prevalidated_dax) covers
    the user's question. First call get_semantic_model_schema to find the right
    table/column/measure references, then pass them here.

    Supports two patterns:
    - **Aggregation** (columns + measures): SUMMARIZECOLUMNS wrapped in CALCULATETABLE
    - **Row-level detail** (select_columns): SELECTCOLUMNS wrapped in CALCULATETABLE

    Returns the assembled DAX query ready to execute via Power BI MCP (ExecuteQuery).
    """
    table = table.strip()
    if not table:
        return "Error: 'table' parameter is required. Use get_semantic_model_schema to find table names."

    # Ensure table is quoted
    if not table.startswith("'"):
        table = f"'{table}'"

    filter_list = [f.strip() for f in filters.split(",") if f.strip()] if filters.strip() else []
    filter_clause = ",\n        ".join(filter_list)

    use_select = bool(select_columns.strip())

    if use_select:
        # ── SELECTCOLUMNS pattern (row-level detail) ─────────────────────
        sc_parts = []
        for pair in select_columns.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if "=" in pair:
                alias, ref = pair.split("=", 1)
                sc_parts.append(f'        "{escape_dax(alias.strip())}", {ref.strip()}')
            else:
                # No alias — use column name as alias
                col_name = pair.split("[")[-1].rstrip("]").strip() if "[" in pair else pair
                sc_parts.append(f'        "{escape_dax(col_name)}", {pair.strip()}')

        sc_block = ",\n".join(sc_parts)

        inner = f"""SELECTCOLUMNS(
        {table},
{sc_block}
    )"""
    else:
        # ── SUMMARIZECOLUMNS pattern (aggregation) ───────────────────────
        col_list = [c.strip() for c in columns.split(",") if c.strip()]
        measure_parts = []
        for pair in measures.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if "=" in pair:
                alias, ref = pair.split("=", 1)
                measure_parts.append(f'            "{escape_dax(alias.strip())}", {ref.strip()}')
            else:
                measure_parts.append(f'            {pair}')

        col_block = ",\n            ".join(col_list) if col_list else ""
        measure_block = ",\n".join(measure_parts) if measure_parts else ""

        sc_items = []
        if col_block:
            sc_items.append(f"            {col_block}")
        if measure_block:
            sc_items.append(measure_block)

        inner = f"""SUMMARIZECOLUMNS(
{chr(10).join(f'{item},' if i < len(sc_items) - 1 else item for i, item in enumerate(sc_items))}
        )"""

    # Wrap in CALCULATETABLE with filters
    if filter_clause:
        body = f"""CALCULATETABLE(
    {inner},
        {filter_clause}
    )"""
    else:
        body = inner

    # Wrap in TOPN if requested
    if top_n > 0:
        order_expr = order_by.strip() if order_by.strip() else "[__first_measure__]"
        # Try to extract first measure alias for default ordering
        if order_expr == "[__first_measure__]":
            if use_select and select_columns.strip():
                first = select_columns.split(",")[0].strip()
                if "=" in first:
                    order_expr = f"[{first.split('=')[0].strip()}]"
                else:
                    order_expr = ""  # Can't infer
            elif measures.strip():
                first = measures.split(",")[0].strip()
                if "=" in first:
                    order_expr = f"[{first.split('=')[0].strip()}]"
                else:
                    order_expr = ""

        if order_expr:
            body = f"""TOPN(
    {top_n},
    {body},
    {order_expr}, DESC
)"""

    # Final ORDER BY (outside TOPN)
    order_suffix = ""
    if order_by.strip():
        order_suffix = f"\nORDER BY {order_by.strip()}"

    dax = f"EVALUATE\n{body}{order_suffix}"

    return f"""## Custom DAX Query
**Semantic Model ID**: `{SEMANTIC_MODEL_ID}`

### Query
```dax
{dax}
```

Execute this via Power BI MCP `ExecuteQuery` with the semantic model ID above.
If the query fails, check column/measure references against `get_semantic_model_schema`."""
