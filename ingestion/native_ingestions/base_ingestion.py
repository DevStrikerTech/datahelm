from typing import Dict, Any, Iterator, List

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dagster import get_dagster_logger

from utils.env_util import PG_HOST, PG_PORT, PG_USER, PG_PASS, PG_DB

log = get_dagster_logger()


class BaseIngestion:
    """
    Base ingestion class for handling common ingestion functionality,
    such as publishing data into a PostgreSQL database.
    """

    def __init__(self) -> None:
        """
        Initializes the BaseIngestion instance.
        Subclasses must call super().__init__() in their __init__.
        """
        pass

    def _publish_to_postgres(
        self,
        table_params: Dict[str, Any],
        record_iter: Iterator[Dict[str, Any]],
    ) -> None:
        """
        Upsert records into a PostgreSQL table. If the target schema or table does not
        exist, they will be created. For each column defined in the YAML configuration,
        if the column is named "id" and its definition includes a "source_key", then the
        column is created as TEXT; otherwise, "id" is created as SERIAL.

        :param table_params: Dictionary containing table parameters with keys:
            - target_db: Name of the target database.
            - target_schema: Name of the target schema.
            - target_table: Name of the target table.
            - columns: List of column definitions, each a dict with at least a "name"
                       key. Optionally, a "source_key" may be provided to map the API field.
        :param record_iter: An iterator over dictionaries representing rows to upsert.
        """
        target_db = table_params.get("target_db", PG_DB)
        schema_name = table_params.get("target_schema")
        table_name = table_params.get("target_table")
        columns_def = table_params.get("columns", [])

        if not schema_name or not table_name:
            log.error("Missing 'target_schema' or 'target_table' in YAML config.")
            return

        if not columns_def:
            log.error("No 'columns' provided in YAML for table: %s", table_name)
            return

        # Build the list of column names from the column definitions.
        # For each column, if its name is "id" and it has a "source_key", then
        # we treat it as a TEXT column; otherwise, if "id" is not mapped, we create it as SERIAL.
        col_names: List[str] = [col_def["name"] for col_def in columns_def]

        # Convert record_iter to a list so we know how many rows we have.
        rows = list(record_iter)
        if not rows:
            log.info("No records to upsert into table: %s.%s", schema_name, table_name)
            return

        # Connect to PostgreSQL using the environment variables.
        conn = None
        try:
            conn = psycopg2.connect(
                dbname=target_db,
                user=PG_USER,
                password=PG_PASS,
                host=PG_HOST,
                port=PG_PORT,
            )
            conn.autocommit = True

            # Ensure the target schema and table exist.
            self._ensure_schema_exists(conn, schema_name)
            self._ensure_table_exists(conn, schema_name, table_name, columns_def)

            # Build the UPSERT SQL.
            insert_cols = ", ".join(col_names)
            conflict_col = "id"  # The primary key is 'id'
            update_assignments = [
                f"{col} = EXCLUDED.{col}" for col in col_names if col != "id"
            ]
            set_clause = ", ".join(update_assignments)

            upsert_sql = sql.SQL("""
                INSERT INTO {schema}.{table} ({insert_cols})
                VALUES %s
                ON CONFLICT ({conflict_col})
                DO UPDATE SET {set_clause}
            """).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
                insert_cols=sql.SQL(insert_cols),
                conflict_col=sql.Identifier(conflict_col),
                set_clause=sql.SQL(set_clause)
            )

            # Build the list of row values in the same order as col_names.
            upsert_data = []
            for rec in rows:
                row_vals = [rec.get(col, None) for col in col_names]
                upsert_data.append(row_vals)

            with conn.cursor() as cur:
                execute_values(cur, upsert_sql.as_string(cur), upsert_data)
                log.info(
                    "Upserted %d rows into %s.%s",
                    len(upsert_data),
                    schema_name,
                    table_name
                )
        except Exception as exc:
            log.error("Error upserting data: %s", exc)
        finally:
            if conn:
                conn.close()

    def _ensure_schema_exists(self, conn, schema_name: str) -> None:
        """
        Ensure that the specified schema exists in PostgreSQL; if not, create it.

        :param conn: psycopg2 connection object.
        :param schema_name: The name of the schema to check/create.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                  FROM information_schema.schemata
                 WHERE schema_name = %s
                """,
                (schema_name,)
            )
            row = cur.fetchone()
            if not row:
                log.info("Schema '%s' not found; creating.", schema_name)
                cur.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
                )
                log.info("Created schema: %s", schema_name)
            else:
                log.info("Schema '%s' exists; skipping creation.", schema_name)

    def _ensure_table_exists(
            self,
            conn,
            schema_name: str,
            table_name: str,
            columns_def: List[Dict[str, Any]]
    ) -> None:
        """
        Ensure that the specified table exists in the schema. If not, create it.
        - If the "id" column is mapped from a source_key, it should be TEXT.
        - Otherwise, "id" defaults to SERIAL (auto-increment).
        - Other columns default to TEXT.

        :param conn: psycopg2 connection object.
        :param schema_name: The name of the schema.
        :param table_name: The name of the table.
        :param columns_def: List of column definitions from the YAML config.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_class c
                  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                  WHERE c.relname = %s
                    AND n.nspname = %s
                    AND c.relkind = 'r'
                )
                """,
                (table_name, schema_name)
            )
            (table_exists,) = cur.fetchone()

            if table_exists:
                log.info("Table %s.%s already exists; skipping creation.", schema_name, table_name)
                return

            log.info("Table %s.%s not found; creating...", schema_name, table_name)

            clauses = []
            primary_key = None

            for col_def in columns_def:
                cname = col_def.get("name")

                if cname == "id":
                    # If "id" is mapped from a source_key (API response), use TEXT
                    if "source_key" in col_def:
                        clause = f"{cname} TEXT NOT NULL"
                    else:
                        clause = f"{cname} SERIAL NOT NULL"
                else:
                    clause = f"{cname} TEXT"

                clauses.append(clause)

                # Set primary key to "id" (or another column if defined)
                if cname == "id":
                    primary_key = cname

            if primary_key:
                clauses.append(f"PRIMARY KEY ({primary_key})")

            col_defs = ", ".join(clauses)

            create_sql = sql.SQL("CREATE TABLE {schema}.{table} ({defs})").format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
                defs=sql.SQL(col_defs)
            )

            cur.execute(create_sql)
            log.info("Created table %s.%s with columns: %s", schema_name, table_name, col_defs)
