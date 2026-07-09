from pyflink.table import TableEnvironment

def register_postgres_sink(t_env: TableEnvironment, table_name: str = "stock_metrics"):
    t_env.execute_sql(f"""
    CREATE TABLE {table_name} (
        symbol STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        avg_price DOUBLE,
        max_price DOUBLE,
        min_price DOUBLE,
        tick_count BIGINT,
        PRIMARY KEY (symbol, window_start) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/stockdb',
        'table-name' = '{table_name}',
        'username' = 'stockuser',
        'password' = 'stockpass',
        'sink.buffer-flush.max-rows' = '1',
        'sink.buffer-flush.interval' = '1s'
    )
    """)
    return table_name