terraform {
  required_version = "= 1.13.5"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "= 3.6.2"
    }
  }
}

provider "docker" {
  host = var.docker_host
}
