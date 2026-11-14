variable "docker_host" {
  description = "Docker endpoint; null uses the provider default/DOCKER_HOST. Windows: npipe:////./pipe/dockerDesktopLinuxEngine."
  type        = string
  default     = null
}

variable "stack_spec" {
  description = "Shared isolated foundation or assessed-stack specification, relative to this module. Use a fresh project/state when selecting the assessed stack."
  type        = string
  default     = "../phase1/stack.json"
  validation {
    condition     = contains(["../phase1/stack.json", "../assessed/stack.json"], var.stack_spec)
    error_message = "Select one of the reviewed isolated stack specifications."
  }
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
  description = "Buildx builder for the provider Docker host. Keep default for the host-based provider client on Linux and Windows; named Docker context builders can conflict with the provider CLI context."
  type        = string
  default     = "default"
}
