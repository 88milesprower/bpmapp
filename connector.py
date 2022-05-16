from snowflake import connector
import yaml

AUTH_CONF_PATH = "auth.yml"


def get_snowflake_connector(auth_path=AUTH_CONF_PATH):
    with open(auth_path) as f:
        auth_info = yaml.safe_load(f)

    ctx = connector.connect(
        **auth_info["snowflake"]
    )

    return ctx, ctx.cursor()
