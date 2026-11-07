"""Destructive migration round-trip check, restricted to the isolated test DB."""

import asyncio
import os
import subprocess

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

url = os.environ["DATABASE_URL"]
if not (make_url(url).database or "").endswith("_test"):
    raise SystemExit("Migration verification requires a database ending with _test")


async def assert_tables(expected):
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            names = set(
                (
                    await connection.execute(
                        text(
                            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                            "AND tablename != 'alembic_version'"
                        )
                    )
                ).scalars()
            )
            assert names == expected, f"Unexpected tables: {names}"
    finally:
        await engine.dispose()


def alembic(*args):
    subprocess.run(["alembic", *args], check=True)


alembic("downgrade", "base")
asyncio.run(assert_tables(set()))
alembic("upgrade", "head")
asyncio.run(assert_tables({"payments", "outbox"}))
alembic("check")
alembic("downgrade", "base")
asyncio.run(assert_tables(set()))
alembic("upgrade", "head")
alembic("check")
print("PASS: empty PostgreSQL -> upgrade -> model check -> downgrade -> upgrade", flush=True)
