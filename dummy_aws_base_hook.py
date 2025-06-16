
# dummy_aws_base_hook.py
# DO NOT place inside any subfolders under /plugins

import sys
import types

DUMMY_MODULE = "airflow.providers.amazon.aws.hooks.base_aws"

# If module is not already mocked, inject it
if DUMMY_MODULE not in sys.modules:
    dummy_module = types.ModuleType(DUMMY_MODULE)

    class AwsBaseHook:
        def __init__(self, *args, **kwargs):
            pass

    dummy_module.AwsBaseHook = AwsBaseHook

    sys.modules[DUMMY_MODULE] = dummy_module
