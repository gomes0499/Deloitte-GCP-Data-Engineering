resource "google_composer_environment" "example_environment" {
  name   = "wu5environment"
  region = "us-central1"

  config {
    node_count = 3

    node_config {
      zone = "us-central1-a"
      machine_type = "n1-standard-1"

      network    = google_compute_network.example_network.self_link
      subnetwork = google_compute_subnetwork.example_subnetwork.self_link
    }

    software_config {
      python_version = "3"
      airflow_config_overrides = {
        core-load_example = "True"
      }
    }
  }
}

resource "google_compute_network" "example_network" {
  name = "wu5network"
}

resource "google_compute_subnetwork" "example_subnetwork" {
  name          = "wu5subnetwork"
  ip_cidr_range = "10.0.0.0/16"
  network       = google_compute_network.example_network.self_link
  region        = "us-central1"
}


