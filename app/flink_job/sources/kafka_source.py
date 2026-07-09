from pyflink.table import TableEnvironment

def register_kafka_source(t_env: TableEnvironment, table_name: str = "stock_prices"):
    t_env.execute_sql(f"""
    CREATE TABLE {table_name} (
        symbol STRING,
        price DOUBLE,
        event_time STRING,
        ts AS TO_TIMESTAMP(REPLACE(event_time, 'T', ' ')),
        WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'stock-prices',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-consumer-final2',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'scan.startup.mode' = 'latest-offset',
        'scan.watermark.idle-timeout' = '5000'
    )
    """)
    return table_name