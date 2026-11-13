"""Cold, coordinated volume backup and restore into a separate Compose project.

Never deletes volumes. Both original and restored evidence are retained. Requires
a previously accepted assessed stack and host Docker access; stdlib only.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
from urllib.request import urlopen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    spec = repo / "infra/assessed/stack.json"
    source = [args.docker, "compose", "-p", "cartco-assessed", "-f", str(spec)]
    project = "cartco-assessed-recovery-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    restored = [args.docker, "compose", "-p", project, "-f", str(spec)]
    helper = "cartco/airflow:assessed"
    verifier = ["exec", "-T", "airflow-scheduler", "/opt/cartco-spark/bin/python", "/opt/airflow/scripts/verify_assessed_catalog.py"]

    def run(command, name):
        print("Running:", name, flush=True)
        with (output / name).open("wb") as stream:
            subprocess.run(command, cwd=repo, stdout=stream, stderr=subprocess.STDOUT, check=True)
        return (output / name).read_text(encoding="utf-8", errors="replace")

    def snapshot():
        with urlopen("http://127.0.0.1:18100/api/snapshot", timeout=10) as response:
            return json.load(response)

    def ready():
        end = time.monotonic() + 300
        while time.monotonic() < end:
            try:
                with urlopen("http://127.0.0.1:19120/api/v2/config", timeout=5) as response:
                    if response.status == 200: return snapshot()
            except (OSError, ValueError): pass
            time.sleep(2)
        raise TimeoutError("Restored catalog/dashboard not ready")

    accepted = snapshot()
    if not accepted.get("quality", {}).get("success") or accepted.get("lineage_transport", {}).get("status") != "verified":
        raise ValueError("Recovery requires a previously quality/lineage-verified publication")
    run(source + verifier, "original-catalog.log")
    specification = json.loads(spec.read_text())
    # Compose does not create volumes used only by an inactive optional profile.
    # Every volume used by an active service is mandatory; missing ones fail.
    names = sorted({mount.split(":")[0]
                    for service in specification["services"].values() if not service.get("profiles")
                    for mount in service.get("volumes", [])})
    volumes = {}
    for key in names:
        name = "cartco-assessed_" + key
        detail = json.loads(run([args.docker, "volume", "inspect", name], key + "-volume.json"))[0]
        if detail["Labels"].get("com.docker.compose.project") != "cartco-assessed":
            raise ValueError("Volume is outside assessed project: " + name)
        volumes[key] = name
    # Refuse reuse of a recovery target; no overwrite or cleanup of prior data.
    existing = run([args.docker, "volume", "ls", "--format", "{{.Name}}"], "existing-volumes.log").splitlines()
    if any(name.startswith(project + "_") for name in existing):
        raise ValueError("Recovery target already exists")
    stopped = False
    recovery_started = False
    manifest = {"project": project, "created_at": datetime.now(timezone.utc).isoformat(),
                "method": "coordinated cold volume archive", "volumes": {}}
    try:
        stopped = True
        run(source + ["stop"], "stop-original.log")
        for key, name in volumes.items():
            archive = output / (key + ".tar.gz")
            code = "import tarfile,sys; t=tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz'); t.add('/volume',arcname='.'); t.close()"
            with archive.open("wb") as stream, (output / (key + "-backup.log")).open("wb") as errors:
                subprocess.run([args.docker, "run", "--rm", "--network", "none", "--user", "0", "--entrypoint", "python",
                    "-v", name + ":/volume:ro", helper, "-c", code], stdout=stream, stderr=errors, check=True)
            with archive.open("rb") as stream:
                checksum = hashlib.file_digest(stream, "sha256").hexdigest()
            with tarfile.open(archive, "r:gz") as contents:
                payload_bytes = sum(member.size for member in contents if member.isfile())
            manifest["volumes"][key] = {"source": name, "archive": archive.name,
                "bytes": archive.stat().st_size, "payload_bytes": payload_bytes, "sha256": checksum}
        (output / "backup-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        if shutil.disk_usage(output).free < sum(item["payload_bytes"] for item in manifest["volumes"].values()) + 1024**3:
            raise RuntimeError("Insufficient recovery headroom; archives and original volumes retained")
        # Create empty labelled volumes directly: creating service containers
        # first could copy image defaults into these destinations.
        for key in volumes:
            run([args.docker, "volume", "create", "--label", "com.docker.compose.project=" + project,
                 "--label", "com.docker.compose.volume=" + key, project + "_" + key], key + "-create.log")
        for key, item in manifest["volumes"].items():
            archive = output / item["archive"]
            with archive.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]:
                    raise ValueError("Backup checksum changed: " + key)
            code = """import tarfile,sys,pathlib
p=pathlib.Path('/volume')
assert not list(p.iterdir()),'restore volume not empty'
def safe(member, destination):
    checked=tarfile.data_filter(member,destination)
    return checked.replace(uid=member.uid,gid=member.gid) if checked else None
with tarfile.open(fileobj=sys.stdin.buffer,mode='r|gz') as archive:
    archive.extractall('/volume',filter=safe)
"""
            with archive.open("rb") as stream, (output / (key + "-restore.log")).open("wb") as log:
                subprocess.run([args.docker, "run", "--rm", "-i", "--network", "none", "--user", "0", "--entrypoint", "python",
                    "-v", project + "_" + key + ":/volume", helper, "-c", code], stdin=stream, stdout=log, stderr=subprocess.STDOUT, check=True)
        recovery_started = True
        run(restored + ["up", "-d", "--no-build"], "start-recovery.log")
        if ready() != accepted: raise ValueError("Restored public snapshot differs")
        run(restored + verifier, "restored-catalog.log")
    finally:
        # Restore the user's original service availability, retaining all copies.
        if recovery_started: run(restored + ["stop"], "stop-recovery.log")
        if stopped:
            run(source + ["up", "-d", "--no-build"], "restart-original.log")
            if ready() != accepted: raise ValueError("Original publication changed during recovery proof")
    (output / "SUCCESS.txt").write_text("Coordinated cold backup restored into separate volumes; published snapshot, catalog identities and exact rows verified; original service availability restored.\n", encoding="utf-8")
    print("Recovery proof passed:", output, flush=True)


if __name__ == "__main__": main()
