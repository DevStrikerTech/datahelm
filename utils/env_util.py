"""
Global environment configuration using environs.
Loads environment variables from .env or the system environment,
exposing typed constants for various credentials/parameters.
"""

from environs import Env

env = Env()
env.read_env()

# Database
PG_HOST = env.str("DB_HOST")
PG_PORT = env.int("DB_PORT")
PG_USER = env.str("DB_USER")
PG_PASS = env.str("DB_PASSWORD")
PG_DB = env.str("DB_NAME")

# API
COC_API_TOKEN_ENV = env.str("CLASHOFCLANS_API_TOKEN")
