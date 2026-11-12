# One shared specification drives Compose and Terraform. This provisions Docker
# resources on an existing host; it does not provision a cloud VM/account.
locals {
  spec      = jsondecode(file("${path.module}/../phase1/stack.json"))
  repo_root = abspath("${path.module}/../..")
  builds = {
    for name, service in local.spec.services : service.image => service.build
    if can(service.build)
  }
  pulls = toset([
    for service in values(local.spec.services) : service.image
    if !contains(keys(local.builds), service.image)
  ])
  active_services = {
    for name, service in local.spec.services : name => service
    if name != "spark-smoke" || var.enable_acceptance
  }
  image_ids = merge(
    { for name, image in docker_image.pulled : name => image.image_id },
    { for name, image in docker_image.built : name => image.image_id }
  )
}

resource "docker_network" "stack" {
  name = "${var.project_name}-network"
}

resource "docker_volume" "persistent" {
  for_each = local.spec.volumes
  name     = "${var.project_name}-${each.key}"
  lifecycle {
    prevent_destroy = true
  }
}

resource "docker_image" "airflow_base" {
  name         = local.spec["x-base-build"].image
  keep_locally = true
  triggers = {
    source = sha256(join("", [filesha256("${local.repo_root}/Dockerfile.airflow"), filesha256("${local.repo_root}/requirements/airflow.txt")]))
  }
  build {
    context    = local.repo_root
    dockerfile = "${local.repo_root}/${local.spec["x-base-build"].dockerfile}"
    builder    = var.buildx_builder
    platform   = "linux/amd64"
  }
}

resource "docker_image" "built" {
  for_each     = local.builds
  name         = each.key
  keep_locally = true
  triggers = {
    source = sha256(join("", concat(
      [filesha256("${local.repo_root}/${each.value.dockerfile}"), filesha256("${local.repo_root}/scripts/phase1_spark_smoke.py"), filesha256("${local.repo_root}/scripts/initialize_airflow.py")],
      [for name in sort(tolist(fileset("${local.repo_root}/infra/phase1", "**"))) : filesha256("${local.repo_root}/infra/phase1/${name}") if !strcontains(name, "__pycache__/") && !endswith(name, ".pyc")]
    )))
  }
  build {
    context    = local.repo_root
    dockerfile = "${local.repo_root}/${each.value.dockerfile}"
    builder    = var.buildx_builder
    platform   = "linux/amd64"
  }
  depends_on = [docker_image.airflow_base]
}

resource "docker_image" "pulled" {
  for_each     = local.pulls
  name         = each.value
  keep_locally = true
}

resource "docker_container" "service" {
  for_each   = local.active_services
  name       = "${var.project_name}-${each.key}"
  image      = local.image_ids[each.value.image]
  env        = [for key, value in try(each.value.environment, {}) : "${key}=${value}"]
  command    = try(each.value.command, null)
  entrypoint = try(each.value.entrypoint, null)
  memory     = tonumber(trimsuffix(each.value.mem_limit, "m"))
  restart    = each.value.restart
  # Initializers and the acceptance job are successful when they exit zero.
  must_run       = !contains(["bucket-init", "airflow-init", "spark-smoke"], each.key)
  remove_volumes = false
  networks_advanced {
    name    = docker_network.stack.name
    aliases = [each.key]
  }
  dynamic "volumes" {
    for_each = try(each.value.volumes, [])
    content {
      volume_name    = docker_volume.persistent[split(":", volumes.value)[0]].name
      container_path = split(":", volumes.value)[1]
    }
  }
  dynamic "ports" {
    for_each = try(each.value.ports, [])
    content {
      ip       = split(":", ports.value)[0]
      external = tonumber(split(":", ports.value)[1])
      internal = tonumber(split(":", ports.value)[2])
    }
  }
  dynamic "healthcheck" {
    for_each = can(each.value.healthcheck) ? [each.value.healthcheck] : []
    content {
      test         = healthcheck.value.test
      interval     = healthcheck.value.interval
      timeout      = healthcheck.value.timeout
      retries      = healthcheck.value.retries
      start_period = healthcheck.value.start_period
    }
  }
}
