resource "google_pubsub_topic" "anomaly_topic" {
  name = "telemetry-anomalies"
}

resource "google_pubsub_subscription" "anomaly_sub" {
  name  = "telemetry-anomalies-sub"
  topic = google_pubsub_topic.anomaly_topic.name
}
