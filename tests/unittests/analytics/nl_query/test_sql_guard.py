import pytest

from analytics.nl_query.sql_guard import SQLSafetyError, ensure_safe_readonly_sql


def test_sql_guard_allows_select_with_limit():
    sql = "SELECT id, name FROM clashofclans_analytics.fct_clashofclans_player_stats LIMIT 100"
    assert ensure_safe_readonly_sql(sql) == sql


def test_sql_guard_blocks_non_select_statements():
    with pytest.raises(SQLSafetyError, match="Only SELECT/CTE queries are allowed"):
        ensure_safe_readonly_sql("DELETE FROM foo WHERE id = 1")


def test_sql_guard_requires_limit():
    with pytest.raises(SQLSafetyError, match="must include a LIMIT"):
        ensure_safe_readonly_sql("SELECT * FROM foo")


def test_sql_guard_blocks_high_limit():
    with pytest.raises(SQLSafetyError, match="exceeds allowed"):
        ensure_safe_readonly_sql("SELECT * FROM foo LIMIT 5000", max_limit=1000)


def test_sql_guard_blocks_forbidden_keyword_inside_query():
    with pytest.raises(SQLSafetyError, match="forbidden statement"):
        ensure_safe_readonly_sql("SELECT * FROM foo LIMIT 10; DROP TABLE foo;")
