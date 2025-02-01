terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.18.0"
    }
  }
}

provider "google" {
    credentials = "./keys/my-creds.json"
  project=""
  region="us-east2"
}