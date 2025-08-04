module "wf-pubsub-factory-topicsource-push" {
  source  = "localterraform.com/TFE-GCP-shared/wf-pubsub-factory/google"
  version = "4.0.1"
  for_each = toset(local.topics)  # or similar
  ...
  create_topic = true
  # Do NOT add push_config, ack_deadline_seconds, etc. here
}

resource "google_pubsub_subscription" "push_subscriptions" {
  for_each = {
    for k, v in local.topics : k => v if v.subscription_type == "push"
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
  max_delivery_attempts        = 5
  dead_letter_topic            = null
  minimum_backoff              = "10s"
  maximum_backoff              = "600s"
  filter                       = "attributes.name = \"plsub\""
  enable_message_ordering      = false
  service_account              = "service-${data.google_project.google_project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  expiration_policy            = "300000.5s"

  subscriptions_labels = {
    environment                 = var.environment
    app_id                      = var.app_id
    au                          = var.au
    application_classification  = var.application_classification
    ci_environment              = var.ci_environment
  }
}
