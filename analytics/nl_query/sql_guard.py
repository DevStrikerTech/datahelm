import re


class SQLSafetyError(ValueError):
    """Raised when generated SQL fails safety checks."""


_FORBIDDEN_PATTERNS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bCREATE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCOPY\b",
    r"\bCALL\b",
]


def ensure_safe_readonly_sql(sql_text: str, max_limit: int = 1000) -> str:
    """
    Guardrail for LLM-generated SQL:
    - Must start with SELECT or WITH
    - Must not contain write/admin statements
    - Must include LIMIT <= max_limit
    """
    if not sql_text or not sql_text.strip():
        raise SQLSafetyError("Generated SQL is empty.")

    compact = sql_text.strip()
    upper = compact.upper()

    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise SQLSafetyError("Only SELECT/CTE queries are allowed.")

    for pattern in _FORBIDDEN_PATTERNS:
        if re.search(pattern, upper):
            raise SQLSafetyError("Generated SQL contains forbidden statement.")

    limit_match = re.search(r"\bLIMIT\s+(\d+)\b", upper)
    if not limit_match:
        raise SQLSafetyError("Generated SQL must include a LIMIT clause.")

    limit_value = int(limit_match.group(1))
    if limit_value > max_limit:
        raise SQLSafetyError(f"LIMIT {limit_value} exceeds allowed max of {max_limit}.")

    return compact
