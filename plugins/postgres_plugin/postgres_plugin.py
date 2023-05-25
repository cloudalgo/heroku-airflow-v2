from airflow.plugins_manager import AirflowPlugin
from postgres_plugin.postgres_service import create_tables_dag


class PostgresPlugin(AirflowPlugin):
    name = "postgres_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
    hooks = []
    executors = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

    # Register the service function as an Airflow macro
    macros = [create_tables_dag]
