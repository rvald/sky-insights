from pyflink.table import EnvironmentSettings,  DataTypes, StreamTableEnvironment
from pyflink.table.udf import udtf
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.window import Slide

# -----------------------------------------------------------------------------
# Define Kafka Source Table for the JSON data
# -----------------------------------------------------------------------------
def create_posts_source_kafka(t_env):
    table_name = "posts"
    source_ddl = f"""
        CREATE TABLE {table_name}  (
            did STRING,
            time_us BIGINT,
            kind STRING,
            operation STRING,
            created_at TIMESTAMP(3),
            langs ARRAY<STRING>,
            text STRING,
            cid STRING,
            event_watermark AS TO_TIMESTAMP_LTZ(time_us, 3),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bluesky-raw-posts',
            'properties.bootstrap.servers' = 'localhost:9092',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """
    t_env.execute_sql(source_ddl)
    return table_name

# -----------------------------------------------------------------------------
# Define PostgreSQL Sink Tables 
# -----------------------------------------------------------------------------
def create_posts_aggregated_sink(t_env):
    table_name = "processed_posts_aggregated"
    sink_ddl_hashtags = f"""
        CREATE TABLE {table_name} (
            hashtag STRING,
            window_end TIMESTAMP(3),
            count BIGINT,
            PRIMARY KEY (hashtag,  window_end) 
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl_hashtags)
    return table_name

# -----------------------------------------------------------------------------
# Trending Hashtags Analysis 
# -----------------------------------------------------------------------------
def trending_hashtag_job():

    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # -----------------------------------------------------------------------------
    # Define UDTFs for Extracting Hashtags
    # -----------------------------------------------------------------------------
    @udtf(result_types=[DataTypes.STRING(), DataTypes.TIMESTAMP(3)])
    def extract_hashtags(text, created_at):
        """
        For each post, splits the text into tokens and emits those starting with '#'
        along with the createdAt timestamp.
        """
        if text is not None:
            for token in text.split():
                if token.startswith('#'):
                    yield token, created_at

    t_env.create_temporary_function("extract_hashtags", extract_hashtags)
    
    # -----------------------------------------------------------------------------
    #  Trending Hashtags Analysis 
    # -----------------------------------------------------------------------------
    # Extract hashtags from posts and assign event time from createdAt.

    try:

        source_table = create_posts_source_kafka(t_env=t_env)
        aggregated_table = create_posts_aggregated_sink(t_env=t_env)

        hashtag_table = source_table.alias("s") \
            .join_lateral("extract_hashtags(s.text, s.created_at) as hashtag, event_time") \
            .select("hashtag, event_time")

        # Apply a sliding window over event_time (10 minutes window sliding every 1 minute) 
        # and count appearances of each hashtag.
        hashtag_agg = hashtag_table.window(
            Slide.over("10.minutes").every("1.minute").on("event_time").alias("w")
        ).group_by("hashtag, w") \
        .select("hashtag, w.end as window_end, COUNT(hashtag) as count")

        hashtag_agg.execute_insert(aggregated_table).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

    if __name__ == '__main__':
        trending_hashtag_job()