output "gke_service_account" {
  value       = google_service_account.gke_sa.email
  description = "The google service account used in GKE"
}

output "k8s_namespace" {
  value       = kubernetes_namespace.k8s_ns.metadata[0].name
  description = "kubernetes namespace"
}

output "k8s_service_account" {
  value       = kubernetes_service_account.k8s_sa.metadata[0].name
  description = "The name of provisioned kubernetes service account"
}
