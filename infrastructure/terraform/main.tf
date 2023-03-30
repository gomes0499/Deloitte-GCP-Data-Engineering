module "gcp-storage" {
    source = "./modules/gcp-storage"
}

module "gcp-sql" {
    source = "./modules/gcp-sql"
}

module "gcp-bigquery" {
    source = "./modules/gcp-bigquery"
}
