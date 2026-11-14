"""Fail closed on migration/user errors; preserve an existing local administrator."""
import json
import os
import subprocess


def initialize(runner=subprocess.run, environ=None):
    env = os.environ if environ is None else environ

    def run(arguments, stage):
        result = runner(["airflow", *arguments], capture_output=True, text=True, check=False)
        if result.returncode:
            # Never include the password-bearing command or captured output.
            raise RuntimeError(f"Airflow {stage} failed (exit {result.returncode})")
        return result.stdout

    run(["db", "migrate"], "database migration")
    output = run(["users", "list", "--output", "json"], "user listing")
    try:
        users = json.loads(output)
        if not isinstance(users, list) or any(not isinstance(user, dict) or not isinstance(user.get("username"), str) for user in users):
            raise ValueError("Invalid user list")
    except (ValueError, TypeError) as error:
        raise RuntimeError("Airflow user listing returned invalid JSON data") from error
    username = env.get("CARTCO_AIRFLOW_ADMIN_USERNAME", "airflow")
    existing = next((user for user in users if user["username"] == username), None)
    if existing is not None:
        print("Airflow database migrated; existing user preserved")
        return
    run(["users", "create", "--username", username,
         "--password", env.get("CARTCO_AIRFLOW_ADMIN_PASSWORD", "airflow"),
         "--firstname", "CartCo", "--lastname", "Admin", "--role", "Admin",
         "--email", env.get("CARTCO_AIRFLOW_ADMIN_EMAIL", "admin@cartco.local")], "administrator creation")
    print("Airflow database migrated; local administrator created")


if __name__ == "__main__":
    initialize()
