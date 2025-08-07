environment_variables = {
    DEST_PROJECT_ID        = var.dest_project_id
    DEST_BUCKET            = var.dest_bucket
    DEST_PATH              = var.dest_path
    SOURCE_PROJECT_ID      = var.source_project_id
    SOURCE_BUCKET          = var.source_bucket
    NOTIFICATION_PROJECT_ID = var.notification_project_id
    NOTIFICATION_TOPIC     = var.notification_topic
    STS_COMPLETION_TOPIC   = var.sts_completion_topic
    DAG_TRIGGER_TOPIC      = var.dag_trigger_topic
  }
