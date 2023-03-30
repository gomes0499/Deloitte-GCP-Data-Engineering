resource "google_storage_bucket" "wu5dataflow" {
  name          = "wu5dataflow"
  location      = "US"
  force_destroy = true
}

resource "google_storage_bucket_object" "python_script" {
  name   = "data-process.py"
  bucket = google_storage_bucket.wu5dataflow.name
  source = "../../../scripts/data-process.py"
}


resource "google_dataflow_job" "data_transformation" {
  name        = "data-transformation"
  project     = "wu5projectgcp"
  region      = "us-central1"
  template_gcs_path = "gs://dataflow-templates/latest/Python3/beam:latest"
  temp_gcs_location = "gs://${google_storage_bucket.wu5dataflow.name}/temp"

  parameters = {
    input = "gs://wu5raw/*"
    output = "gs://wu5process/"
    main_python_file = "gs://wu5dataflow/script.py"
  }
  on_delete = "cancel"
}
