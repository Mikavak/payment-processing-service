"""Check tracked and non-ignored new files for local credentials before committing."""

import re
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]
paths = (
    subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    )
    .decode()
    .split("\0")
)
local_secrets = []
if (root / ".env").exists():
    for line in (root / ".env").read_text().splitlines():
        name, _, value = line.partition("=")
        if any(token in name for token in ("KEY", "PASSWORD", "TOKEN", "SECRET")) and value:
            local_secrets.append(value)

problems = []
for relative in filter(None, paths):
    path = root / relative
    if (path.name == ".env" or path.name.startswith(".env.")) and path.name != ".env.example":
        problems.append(f"Environment file included: {relative}")
    if not path.is_file():
        continue
    content = path.read_text(errors="replace")
    if any(value in content for value in local_secrets):
        problems.append(f"Local credential found: {relative}")
    if re.search(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----", content):
        problems.append(f"Private key found: {relative}")
if problems:
    raise SystemExit("\n".join(problems))
print("PASS: no .env files, local credentials or private keys in Git candidate files")
