provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_pubsub_topic" "anomaly_topic" {
  name = "telemetry-anomalies"
}

resource "google_pubsub_subscription" "anomaly_sub" {
  name  = "telemetry-anomalies-sub"
  topic = google_pubsub_topic.anomaly_topic.name
}

resource "google_bigtable_instance" "telemetry_instance" {
  name         = "fleet-telemetry-bt"
  cluster {
    cluster_id   = "telemetry-cluster"
    zone         = "${var.region}-a"
    num_nodes    = 3
    storage_type = "SSD"
  }
}

resource "google_bigtable_table" "telemetry_table" {
  name          = "telemetry_frames"
  instance_name = google_bigtable_instance.telemetry_instance.name

  column_family {
    family = "sys"
  }
  column_family {
    family = "net"
  }
  column_family {
    family = "meta"
  }
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}
