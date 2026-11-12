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
