
# dummy_aws_base_hook.py
# Place this in your Composer environment under /dags/plugins/

import sys
import types

# Create dummy module path
aws_module = types.ModuleType("airflow.providers.amazon.aws.hooks.base_aws")
sys.modules["airflow.providers.amazon.aws.hooks.base_aws"] = aws_module

# Define dummy AwsBaseHook
class AwsBaseHook:
    def __init__(self, *args, **kwargs):
        pass

aws_module.AwsBaseHook = AwsBaseHook
