output "project" {
  value = var.project_name
}

output "local_endpoints" {
  value = {
    airflow    = "http://127.0.0.1:18080"
    nessie_api = "http://127.0.0.1:19120/api/v2"
    minio      = "http://127.0.0.1:19001"
    marquez    = "http://127.0.0.1:15000"
    lineage_ui = "http://127.0.0.1:13000"
  }
}
