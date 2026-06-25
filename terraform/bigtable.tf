resource "google_bigtable_instance" "telemetry_instance" {
  name = "fleet-telemetry-bt"
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
