variable "docker_host" {
  description = "Docker endpoint; null uses the provider default/DOCKER_HOST. Windows: npipe:////./pipe/dockerDesktopLinuxEngine."
  type        = string
  default     = null
}

variable "project_name" {
  description = "Separate from Compose and all baseline/recovery resources."
  type        = string
  default     = "cartco-infra-p1-tf"
  validation {
    condition     = can(regex("^cartco-infra-p1-tf(-[a-z0-9]+)?$", var.project_name))
    error_message = "Use cartco-infra-p1-tf with an optional isolated lowercase suffix."
  }
}

variable "enable_acceptance" {
  description = "Start the Spark smoke container after provisioning. Run the host acceptance script for restart proof."
  type        = bool
  default     = false
}

variable "buildx_builder" {
  description = "Existing Buildx builder for this Docker host. Linux commonly uses default; Docker Desktop commonly uses desktop-linux. Must target the same engine as docker_host."
  type        = string
  default     = "default"
}
