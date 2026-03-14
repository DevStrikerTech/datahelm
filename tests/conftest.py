import os


# Ensure required environment variables exist for module import-time config.
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")
os.environ.setdefault("DB_NAME", "postgres")
os.environ.setdefault("CLASHOFCLANS_API_TOKEN", "test-token")
