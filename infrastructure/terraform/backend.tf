terraform {
  backend "gcs" {
    bucket  = "wu5tfstate"
    prefix  = "terraform/state"
  }
}
