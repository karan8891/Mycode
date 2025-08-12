# gs://.../plugins/dummy_aws_base_hook.py
import sys, types

# Ensure parent namespaces exist
for name in [
    "airflow.providers",
    "airflow.providers.amazon",
    "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.hooks",
]:
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)

# Create the target module and symbol expected by google provider
module_name = "airflow.providers.amazon.aws.hooks.base_aws"
if module_name not in sys.modules:
    m = types.ModuleType(module_name)
    class AwsBaseHook:  # minimal stub
        def __init__(self, *args, **kwargs): ...
    m.AwsBaseHook = AwsBaseHook
    sys.modules[module_name] = m

print(" Dummy AwsBaseHook plugin loaded")
