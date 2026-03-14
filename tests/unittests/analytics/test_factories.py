import pytest

from analytics.dashboard_factory import DashboardFactory
from analytics.dbt_factory import DbtProjectFactory, _default_target_schema


def test_default_target_schema_normalizes_source_name():
    assert _default_target_schema("CLASHOFCLANS_SOURCE") == "clashofclans_analytics"
    assert _default_target_schema("  weird---name  ") == "weird_name_analytics"
    assert _default_target_schema("___") == "source_analytics"


def test_dbt_project_factory_loads_units_and_merges_defaults(tmp_path):
    config_dir = tmp_path / "dbt"
    config_dir.mkdir()
    (config_dir / "projects.yaml").write_text(
        """
base_dbt_project:
  is_active: true
  profile_name: datahelm
  target: dev
  profiles_dir: analytics/dbt_profiles
  dbt_command: build
  select:
    - tag:clashofclans
  vars:
    source_schema: clashofclans

CLASHOFCLANS_SOURCE:
  is_active: true
  project_dir: analytics/dbt_projects/datahelm_warehouse
  profile_name: ${base_dbt_project.profile_name}
  target: ${base_dbt_project.target}
  profiles_dir: ${base_dbt_project.profiles_dir}
  dbt_command: ${base_dbt_project.dbt_command}
  select: ${base_dbt_project.select}
  vars: ${base_dbt_project.vars}
  units:
    STAGING:
      is_active: true
      select:
        - path:models/staging
    MARTS:
      is_active: false
      exclude:
        - path:models/staging
""".strip(),
        encoding="utf-8",
    )

    factory = DbtProjectFactory(config_dir=str(config_dir))

    staging = factory.get_unit("CLASHOFCLANS_SOURCE_STAGING")
    marts = factory.get_unit("CLASHOFCLANS_SOURCE_MARTS")

    assert staging["is_active"] is True
    assert staging["project_dir"] == "analytics/dbt_projects/datahelm_warehouse"
    assert staging["select"] == ["path:models/staging"]
    assert staging["vars"]["source_schema"] == "clashofclans"
    assert staging["vars"]["target_schema"] == "clashofclans_analytics"

    assert marts["is_active"] is False
    assert marts["exclude"] == ["path:models/staging"]
    assert marts["dbt_command"] == "build"


def test_dbt_project_factory_raises_for_unknown_unit(tmp_path):
    config_dir = tmp_path / "dbt"
    config_dir.mkdir()
    (config_dir / "projects.yaml").write_text(
        """
EXAMPLE_SOURCE:
  project_dir: analytics/dbt_projects/datahelm_warehouse
  units:
    STAGING:
      is_active: true
""".strip(),
        encoding="utf-8",
    )

    factory = DbtProjectFactory(config_dir=str(config_dir))

    with pytest.raises(KeyError, match="DBT unit 'MISSING_UNIT' not found"):
        factory.get_unit("MISSING_UNIT")


def test_dashboard_factory_loads_units_with_fallbacks(tmp_path):
    config_dir = tmp_path / "dashboard"
    config_dir.mkdir()
    (config_dir / "projects.yaml").write_text(
        """
base_dashboard:
  notebook_path: analytics/notebooks/source_overview.ipynb
  schedules:
    - cron_schedule: "45 */4 * * *"
      execution_timezone: "UTC"
      default_status: RUNNING

CLASHOFCLANS_DASHBOARD:
  is_active: true
  notebook_path: ${base_dashboard.notebook_path}
  schedules: ${base_dashboard.schedules}
  units:
    OVERVIEW:
      is_active: true
      db_schema: clashofclans_analytics
      db_table: fct_clashofclans_player_stats
      chart_x_col: name
      chart_y_col: attackwins
    DISABLED:
      is_active: false
      db_schema: clashofclans_analytics
      db_table: fct_clashofclans_player_stats
      chart_x_col: name
      chart_y_col: defensewins
      row_limit: 10
""".strip(),
        encoding="utf-8",
    )

    factory = DashboardFactory(config_dir=str(config_dir))

    overview = factory.get_unit("CLASHOFCLANS_DASHBOARD_OVERVIEW")
    disabled = factory.get_unit("CLASHOFCLANS_DASHBOARD_DISABLED")

    assert overview["is_active"] is True
    assert overview["row_limit"] == 25
    assert overview["notebook_path"] == "analytics/notebooks/source_overview.ipynb"
    assert overview["schedules"][0]["cron_schedule"] == "45 */4 * * *"

    assert disabled["is_active"] is False
    assert disabled["row_limit"] == 10


def test_dashboard_factory_raises_for_unknown_unit(tmp_path):
    config_dir = tmp_path / "dashboard"
    config_dir.mkdir()
    (config_dir / "projects.yaml").write_text(
        """
CLASHOFCLANS_DASHBOARD:
  notebook_path: analytics/notebooks/source_overview.ipynb
  units:
    OVERVIEW:
      db_schema: clashofclans_analytics
      db_table: fct_clashofclans_player_stats
      chart_x_col: name
      chart_y_col: attackwins
""".strip(),
        encoding="utf-8",
    )

    factory = DashboardFactory(config_dir=str(config_dir))

    with pytest.raises(KeyError, match="Dashboard unit 'MISSING_UNIT' not found"):
        factory.get_unit("MISSING_UNIT")
