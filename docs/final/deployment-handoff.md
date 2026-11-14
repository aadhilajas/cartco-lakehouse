# Deployment and publication handoff

Updated: 2026-11-11. The assignment-aligned plan supersedes historical optional
Spark/IaC and local-only scope interpretations.

The user authorizes publishing reviewed code and the dashboard when ready.
All four connectors and a hosted backend are required for the 1 December viva.
Budget is zero; there is no existing server. See
[hosting feasibility](hosting-feasibility.md) for the unresolved hosting gate.
No cloud deployment or final remote CI execution is claimed by local proof.

Before deployment, verify free account capacity, architecture compatibility,
measured memory, persistent storage, HTTPS/access controls and recovery. Replace
local fixture credentials; do not expose backend administrative or write APIs.
Terraform and Compose must not concurrently own the same resources.

Before publishing code, review the branch and secret scan, resolve GitHub write
access if needed, and run CI against the actual final commit. Record genuine run
URLs; historical green CI does not validate new changes. Publication authorization
need not be requested again for ready reviewed code/dashboard.

Record the final walkthrough against the completed build and add the actual URL.
Written scripts are not proof of a published video. Paid services, signatures,
external messages and form submissions are not authorized by this handoff.
