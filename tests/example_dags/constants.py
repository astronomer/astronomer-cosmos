import os

if os.getenv("CI_ENABLED"):
    TEST_CONNECTIONS_YAML_FILE = "tests/ci-test-connections.yaml"
else:
    TEST_CONNECTIONS_YAML_FILE = "tests/local-test-connections.yaml"
