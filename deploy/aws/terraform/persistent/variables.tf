variable "region" {
  type    = string
  default = "us-east-2"
}

variable "account_id" {
  type    = string
  default = "493469777520"
}

variable "cluster_name" {
  type    = string
  default = "sdh-poc"
}

# One namespace and one set of keys per pipeline, as in the sandbox.
variable "pipelines" {
  type    = list(string)
  default = ["ccda-push"]
}

variable "images" {
  type    = list(string)
  default = ["landing-connector", "split-stage", "convert-stage", "transform-stage"]
}

# Volume name => size in GiB. Kafka holds only the 2-4 KB announcements, not documents (DESIGN.md §11).
# GiB. 10 each holds the PoC's runs of 90,000 documents: about 1 million documents in Postgres (9.6 KB a document in
# the stage registries) and 500,000 in Kafka (21 KB of messages a document, kept 30 days), from run 53. The 15-million
# initial load needs about 200 and 350 (docs/PERFORMANCE.md §11.1), about $44 a month in gp3 while they exist, since
# they outlive each session. An EBS volume can grow in place but never shrink.
variable "state_volumes" {
  type    = map(number)
  default = { postgres = 10, kafka = 10 }
}

variable "state_zone" {
  type    = string
  default = "us-east-2a"
}

# Kubernetes service accounts for each stage's role: "<namespace>/<service account>".
variable "stage_service_accounts" {
  type = map(string)
  default = {
    landing   = "sdh-ccda-push/landing"
    splitter  = "sdh-ccda-push/splitter"
    convert   = "sdh-ccda-push/convert"
    transform = "sdh-fhir/transform"
  }
}
