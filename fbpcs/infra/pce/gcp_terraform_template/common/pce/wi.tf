locals {
  gke_sa = "onedocker-${var.name_postfix}"
  k8s_ns = kubernetes_namespace.k8s_ns.metadata[0].name
  k8s_sa = kubernetes_service_account.k8s_sa.metadata[0].name
}

resource "google_service_account" "gke_sa" {
  account_id   = local.gke_sa
  display_name = local.gke_sa
  project      = var.project_id
}

resource "kubernetes_namespace" "k8s_ns" {
  metadata {
    name = "onedocker-${var.name_postfix}"
  }
}

resource "kubernetes_service_account" "k8s_sa" {
  metadata {
    name      = "onedocker-k8s-sa-${var.name_postfix}"
    namespace = local.k8s_ns
    annotations = {
      "iam.gke.io/gcp-service-account" : google_service_account.gke_sa.email
    }
  }
}

resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = google_service_account.gke_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${local.k8s_ns}/${local.k8s_sa}]"
}
