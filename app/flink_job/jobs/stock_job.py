from pyflink.table import EnvironmentSettings, TableEnvironment
from app.flink_job.sources.kafka_source import register_kafka_source
from app.flink_job.sinks.postgres_sink import register_postgres_sink

RUN_MODE = "aggregate"

def main():
    print("Creating TableEnvironment...", flush=True)
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    source_table = register_kafka_source(t_env)
    print(f"Kafka source table '{source_table}' registered.", flush=True)

    sink_table = register_postgres_sink(t_env)
    print(f"Postgres sink table '{sink_table}' registered.", flush=True)

    print("Submitting INSERT INTO job...", flush=True)
    result = t_env.execute_sql(f"""
    INSERT INTO {sink_table}
    SELECT
        symbol,
        window_start,
        window_end,
        AVG(price),
        MAX(price),
        MIN(price),
        COUNT(*)
    FROM TABLE(
        TUMBLE(TABLE {source_table}, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
    )
    GROUP BY symbol, window_start, window_end
    """)
    print("Job submitted, waiting for completion...", flush=True)
    result.wait()
    print("Job finished.", flush=True)

if __name__ == "__main__":
    main()