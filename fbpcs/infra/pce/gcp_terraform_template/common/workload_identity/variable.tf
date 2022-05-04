variable "project_id" {
  description = "an unique identifier for your GCP project"
}

variable "name_postfix" {
  description = "the postfix to append after a resource name"
}

variable "gcp_region" {
  description = "the region of the k8s cluster"
}

variable "cluster_name" {
  description = "The cluster in the kubernetes context"
}
