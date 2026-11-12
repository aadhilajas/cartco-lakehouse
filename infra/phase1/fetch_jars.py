"""Build-time artifact downloader; no unverified runtime Maven resolution."""
import hashlib
import json
from pathlib import Path
import sys
from urllib.request import urlopen


def fetch(manifest: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for item in json.loads(manifest.read_text(encoding="utf-8"))["jars"]:
        name = item["name"]
        if Path(name).name != name or not item["url"].startswith("https://repo.maven.apache.org/"):
            raise ValueError("Invalid locked artifact")
        target = destination / name
        digest = hashlib.sha512()
        with urlopen(item["url"], timeout=120) as response, target.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
        if digest.hexdigest() != item["sha512"]:
            target.unlink()
            raise ValueError(f"Artifact checksum mismatch: {name}")
        print(f"Verified {name}", flush=True)


if __name__ == "__main__":
    fetch(Path(sys.argv[1]), Path(sys.argv[2]))
