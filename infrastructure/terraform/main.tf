module "gcp-storage" {
    source = "./modules/gcp-storage"
}

module "gcp-sql" {
    source = "./modules/gcp-sql"
}

# module "gcp-dataflow" {
#     source = "./modules/gcp-dataflow"
# }

# module "gcp-bigquery" {
#     source = "./modules/gcp-bigquery"
# }

# module "gcp-composer" {
#     source = "./modules/gcp-composer"
# }