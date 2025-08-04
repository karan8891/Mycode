resource "google_pubsub_subscription" "push_subscriptions" {
  for_each = {
    for k, v in local.topics : k => v
    if v.subscription_type == "push"
  }

  name  = each.value.subscription_name
  topic = "projects/${var.project_id}/topics/${each.key}"

  push_config {
    push_endpoint = each.value.push_endpoint
    oidc_token {
      service_account_email = each.value.sa_email
    }
  }

  ack_deadline_seconds         = 10
  retain_acked_messages        = true
  message_retention_duration   = "604800s"
}
