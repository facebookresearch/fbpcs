resource "google_compute_network_peering" "vpc_peering" {
  name         = "onedocker-vpc-peering-${var.name_postfix}"
  network      = var.vpc_id
  peer_network = var.otherparty_vpc_id
}
