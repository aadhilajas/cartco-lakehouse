# Validation evidence index

Checkpoint JSON files are **dated records**, not live health endpoints. A pending
item in an older checkpoint can be closed by later evidence without rewriting
the older file. Full local logs, JUnit and recovery archives are not all tracked.

| Summary | Scope |
|---|---|
| [Native](assessed-native-checkpoint.json) | Native protocol/Spark implementation checks |
| [Compose](assessed-compose-checkpoint.json) | Scheduled current stack, table and browser behavior |
| [Cold recovery](assessed-recovery-checkpoint.json) | Eight-volume isolated restore proof |
| [Terraform](assessed-terraform-checkpoint.json) | Expanded stack and convergent local Docker IaC |
| [Final local](final-local-checkpoint.json) | State before GitHub integration and public deployment |
| [Public demo](public-demo-checkpoint.json) | Actual GitHub publication and Vercel endpoint verification |

[Current verification report](../test_report.md) reconciles these stages.
The baseline's older report is [preserved separately](../history/baseline-test-report.md).
A small synthetic fixture and local restart proof are not a production load test,
persistent cloud storage qualification or independent offsite backup.
