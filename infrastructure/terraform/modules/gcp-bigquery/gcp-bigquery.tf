resource "google_bigquery_dataset" "default" {
  dataset_id                  = "wu5datasetid"
  friendly_name               = "wu5bigquery"
  description                 = "This is a test description"
  location                    = "US"

  labels = {
    env = "default"
  }
  access {
    role          = "OWNER"
    user_by_email = google_service_account.bqowner.email
  }

  access {
    role   = "READER"
    domain = "hashicorp.com"
  }
}

resource "google_service_account" "bqowner" {
  account_id = "wu5bqowner"
}