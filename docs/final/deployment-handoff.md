# Deployment and publication handoff

No cloud deployment or final remote CI execution is included in the local proof.
The repository remains local under the user's standing instruction.

Before cloud implementation, select the target account/region and budget, source
and secret ownership, storage/catalog/metadata services, network/TLS design and
backup/alert recipients. The local Compose credentials and exposed endpoints are
not deployment settings. Decide the executor from actual workload requirements.
Terraform was a stretch item in the original design; no unvalidated template is
presented as provisioned infrastructure.

Before publishing the code, review the local branch and complete-history bundle,
authorize the remote update, ensure the integration has contents/workflow write
access, and run the prepared GitHub Actions workflow against the final commit.
Record actual run URLs and outcomes. Do not reuse historical green CI as proof of
new changes. Preserve local test reports while that step is pending.

Before publishing a demo, record the final walkthrough, verify that no secrets
or private data appear, and upload through the user's chosen destination. Add
the actual URL afterward. The written walkthrough is a local deliverable, not a
claim that a final video or hosted service already exists.
