"""Create local credentials without printing them or overwriting an existing .env."""

import os
import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
target = root / ".env"
if target.exists():
    raise SystemExit(".env already exists; left unchanged")

api_key, postgres_password, rabbit_password = [secrets.token_hex(24) for _ in range(3)]
content = (
    f"API_KEY={api_key}\n"
    "POSTGRES_USER=payments\n"
    f"POSTGRES_PASSWORD={postgres_password}\n"
    "POSTGRES_DB=payments\n"
    "RABBITMQ_USER=payments\n"
    f"RABBITMQ_PASSWORD={rabbit_password}\n"
    f"DATABASE_URL=postgresql+asyncpg://payments:{postgres_password}@localhost:5432/payments\n"
    f"RABBITMQ_URL=amqp://payments:{rabbit_password}@localhost:5672/\n"
    "OUTBOX_POLL_INTERVAL=0.5\nRETRY_BASE_DELAY=1\nWEBHOOK_TIMEOUT=5\n"
)
fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write(content)
print("Created .env with random local credentials (mode 0600)")
