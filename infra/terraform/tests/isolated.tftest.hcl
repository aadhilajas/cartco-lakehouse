# Mock-provider plans check wiring without pretending Docker ran.
mock_provider "docker" {}

run "foundation_plan" {
  command = plan
  assert {
    condition     = length(docker_container.service) == 11
    error_message = "Default foundation excludes the one-shot Spark job."
  }
  assert {
    condition     = docker_network.stack.name == "cartco-infra-p1-tf-network"
    error_message = "Terraform must not share baseline/Compose resource names."
  }
  assert {
    condition     = length(docker_volume.persistent) == 6
    error_message = "Persistent stores and evidence must be explicit."
  }
  assert {
    condition     = contains(docker_container.service["nessie"].env, "NESSIE_VERSION_STORE_TYPE=JDBC2")
    error_message = "Nessie must use persistent JDBC storage."
  }
  assert {
    condition     = docker_container.service["nessie"].ports[0].ip == "127.0.0.1"
    error_message = "Local catalog must not bind public host interfaces."
  }
}

run "acceptance_plan" {
  command = plan
  variables {
    enable_acceptance = true
  }
  assert {
    condition     = length(docker_container.service) == 12 && !docker_container.service["spark-smoke"].must_run
    error_message = "Acceptance is an optional finite Spark job."
  }
  assert {
    condition     = docker_container.service["spark-smoke"].memory == 2560
    error_message = "Spark must have a bounded memory budget."
  }
}
