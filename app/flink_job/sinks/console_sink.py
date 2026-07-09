from pyflink.table import TableEnvironment

def run_console_sink(t_env: TableEnvironment, source_table: str):
    t_env.execute_sql(f"SELECT * FROM {source_table}").print()