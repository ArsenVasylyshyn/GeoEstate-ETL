from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.exceptions import AirflowFailException

from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
from airflow.models.connection import Connection
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, trim, regexp_replace, to_date, concat_ws, make_date, floor,
                                  row_number, lit, count, max, min, mean, when, length, year, desc)
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window
from datetime import date, datetime
from tabulate import tabulate
import logging
import os


default_args = {
    'owner': 'arsen_vasylyshyn',
    'depends_on_past': False,
    'start_date': days_ago(1),
}


# 🟢 Create clickhouse connection (idempotent)
@provide_session
def create_clickhouse_connection(session=None, **kwargs):
    conn_id = "clickhouse_default"
    password = os.getenv('CLICKHOUSE_PASSWORD', '')
    existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing:
        logging.info(f"Updating existing ClickHouse connection '{conn_id}'")
        existing.conn_type = "clickhouse"
        existing.host = "clickhouse"  
        existing.port = 9000
        existing.schema = "default"
        existing.login = "default"
        existing.password = password
    else:
        logging.info(f"Creating new ClickHouse connection '{conn_id}'")
        new_conn = Connection(
            conn_id=conn_id,
            conn_type="clickhouse",
            host="clickhouse",
            port=9000,
            schema="default",
            login="default",
            password=password
        )
        session.add(new_conn)

    session.commit()

    try:
        conn_id = "clickhouse_default"  
        conn = BaseHook.get_connection(conn_id)

        logging.info(f"Connecting to ClickHouse at {conn.host}:{conn.port} as {conn.login}")

        client = Client(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password if conn.password else "",
            database=conn.schema if conn.schema else "default"  
        )

        version = client.execute("SELECT version()")[0][0]
        logging.info(f"✅ ClickHouse connected: version {version}")
        return version
    except Exception as e:
        logging.error(f"❌ Connection failed: {e}")
        raise AirflowFailException("ClickHouse connection test failed.")


# 🟡 Get ClickHouse client from Airflow connection
def get_clickhouse_client():
    conn = BaseHook.get_connection("clickhouse_default")
    logging.info(f"📡 Connecting to ClickHouse at {conn.host}:{conn.port} as {conn.login}")
    return Client(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password if conn.password else "",
        database=conn.schema
    )


# 🔵 Create user, grant rights, create table
def create_user_and_table_clickhouse():
    client = get_clickhouse_client()
    conn = BaseHook.get_connection("clickhouse_default")

    try:
        client.execute("CREATE USER IF NOT EXISTS airflow_user IDENTIFIED WITH no_password")
        logging.info("✅ Created user 'airflow_user'")
    except Exception as e:
        logging.warning(f"⚠️ User creation failed: {e}")

    try:
        client.execute("GRANT SELECT, INSERT, CREATE, ALTER, DROP ON default.* TO airflow_user")
        logging.info("✅ Granted rights to 'airflow_user'")
    except Exception as e:
        logging.warning(f"⚠️ Grant failed: {e}")

    try:
        user_client = Client(host=conn.host, port=conn.port, user='airflow_user', password='')
        CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS geo_estate_data (
                house_id Int32,
                latitude Float64,
                longitude Float64,
                maintenance_year Date,
                square Float64,
                population Int32,
                region String,
                locality_name String,
                address String,
                full_address String,
                communal_service_id Int32,
                description String
            ) ENGINE = MergeTree()
            ORDER BY house_id
            """
        user_client.execute(CREATE_TABLE_SQL)
        logging.info("✅ Table 'geo_estate_data' created.")

        # Print schema
        schema = user_client.execute("DESCRIBE TABLE geo_estate_data")
        logging.info("📋 Table schema for 'geo_estate_data':")
        for col in schema:
            logging.info(f" - {col[0]}: {col[1]}")

    except Exception as e:
        logging.error(f"❌ Table creation failed: {e}")


# ⚡️ Create spark session
def create_spark_session():
    return SparkSession.builder.appName("Load CSV to DataFrame").getOrCreate()   


# 🧠 Task 1: Load CSV & Save Parquet
def load_csv_to_dataframe(**kwargs):
    csv_path = "/opt/airflow/data/russian_houses_slice.csv"

    spark = create_spark_session()
    df = spark.read.option("header", "true") \
               .option("encoding", "UTF-16") \
               .option("multiLine", "true") \
               .option("escape", "\"") \
               .option("inferSchema", "true") \
               .csv(csv_path)
    
    row_count = df.count()
    df.write.parquet("/opt/airflow/shared/step1.parquet", mode="overwrite")

    print(f"📊 Number of rows in CSV file: {row_count}")
    # print("Column names:", ", ".join(df.columns))
    # df.printSchema()
    # df.show(20)
    
    spark.stop()


# 🧠 Task 2: Read Parquet & Validate 
def validate_dataframe(**kwargs):
    try:
        spark = create_spark_session()
        df = spark.read.parquet("/opt/airflow/shared/step1.parquet")
        
        # Clean numeric columns
        df = df.withColumn("square", regexp_replace(trim(col("square")), "[^0-9.]", "")) \
               .withColumn("population", regexp_replace(trim(col("population")), "[^0-9]", "")) \
               .withColumn("communal_service_id", regexp_replace(trim(col("communal_service_id")), "[^0-9]", "")) \
               .withColumn("maintenance_year", regexp_replace(trim(col("maintenance_year")), "[^0-9]", "")) \
               .withColumn("latitude", regexp_replace(trim(col("latitude")), "[^0-9.-]", "")) \
               .withColumn("longitude", regexp_replace(trim(col("longitude")), "[^0-9.-]", ""))

        def is_not_empty(column_name):
            return col(column_name).isNotNull() & (trim(col(column_name)) != "")
     
        valid_text_fields = (
            is_not_empty("region") &
            is_not_empty("address") &
            is_not_empty("description") &
            is_not_empty("locality_name") &
            is_not_empty("communal_service_id")
        )

        valid_house_id = col("house_id").cast(IntegerType()).isNotNull()
        valid_coordinates = (
            col("latitude").cast(DoubleType()).isNotNull() &
            col("longitude").cast(DoubleType()).isNotNull()
        )
        valid_year = (
            col("maintenance_year").rlike(r"^[0-9]{4}$") &
            col("maintenance_year").cast(IntegerType()).isNotNull()
        )
        valid_square = (
            col("square").rlike(r"^[0-9]+(\.[0-9]+)?$") &
            col("square").cast(DoubleType()).isNotNull()
        )
        valid_population = (
            col("population").rlike(r"^[0-9]+$") &
            col("population").cast(IntegerType()).isNotNull()
        )
        valid_communal_service_id = (
            col("communal_service_id").rlike(r"^[0-9]+$") &
            col("communal_service_id").cast(IntegerType()).isNotNull()
        )

        is_row_valid = (
            valid_house_id &
            valid_coordinates &
            valid_year &
            valid_square &
            valid_population &
            valid_communal_service_id &
            valid_text_fields
        )

        valid_df = df.filter(is_row_valid).persist()

        window_spec = Window.orderBy("house_id")
        valid_df = valid_df.withColumn("house_id", row_number().over(window_spec))
        

        columns_to_keep = [
            "house_id", "latitude", "longitude", "maintenance_year", "square", "population",
            "region", "locality_name", "address", "full_address", "communal_service_id", "description"
        ]

        valid_df.select(columns_to_keep).write.parquet("/opt/airflow/shared/valid_rows.parquet", mode="overwrite")

        valid_count = valid_df.count()
        total_count = df.count()
        invalid_count = total_count - valid_count

        logger = logging.getLogger("airflow.task")
        logger.info(f"✅ Valid rows: {valid_count}")
        logger.info(f"❌ Invalid rows: {invalid_count}")

        valid_df.unpersist()
        spark.stop()

    except Exception as e:
        logger = logging.getLogger("airflow.task")
        logger.error(f"Validation failed: {str(e)}", exc_info=True)
        raise
   

# 🧠 Task 3: convert_data_types 
def convert_data_types(**kwargs):

    spark = create_spark_session()
    df = spark.read.parquet("/opt/airflow/shared/valid_rows.parquet")
    
    # Casting columns
    df = df \
        .withColumn("house_id", col("house_id").cast(IntegerType())) \
        .withColumn("latitude", spark_round(col("latitude"), 6).cast(DoubleType())) \
        .withColumn("longitude", spark_round(col("longitude"), 6).cast(DoubleType())) \
        .withColumn("maintenance_year", make_date(col("maintenance_year"), lit(1), lit(1))) \
        .withColumn("square", col("square").cast(DoubleType())) \
        .withColumn("population", col("population").cast(IntegerType())) \
        .withColumn("region", col("region").cast(StringType())) \
        .withColumn("locality_name", col("locality_name").cast(StringType())) \
        .withColumn("address", col("address").cast(StringType())) \
        .withColumn("full_address", col("full_address").cast(StringType())) \
        .withColumn("communal_service_id", col("communal_service_id").cast(IntegerType())) \
        .withColumn("description", col("description").cast(StringType())) 
        
    df.write.parquet("/opt/airflow/shared/valid_data_types.parquet", mode="overwrite") 

    print("📊 Valid DataFrame Columns and Types:")
    # for name, dtype in df.dtypes:
    #     print(f"⚡️ {name:<20} | {dtype}")

    df.printSchema()    

    spark.stop()


# 🧠 Task 4: average_median_year
def building_year_stats(**kwargs):

    spark = create_spark_session()
    df = spark.read.parquet("/opt/airflow/shared/valid_data_types.parquet")

    # Extract year from date
    df = df.withColumn("maintenance_year_num", year("maintenance_year"))

    # Average year
    avg_year = df.agg(mean("maintenance_year_num").alias("avg_year")).first()["avg_year"]
    
    # Median year
    median_year = df.selectExpr("percentile_approx(maintenance_year_num, 0.5) as median_year") \
                    .first()["median_year"]
    
    # Output results
    print("Building Year Stats")
    print(f"✅ ➤ Average year built: {round(avg_year)}")
    print(f"✅ ➤ Median year built: {median_year}")

    spark.stop()
 
 
# 🧠 Task 5: top_regions_and_cities
def top_regions_and_cities(**kwargs):

    spark = create_spark_session()
    df = spark.read.parquet("/opt/airflow/shared/valid_data_types.parquet")

    # Top 10 region
    top_region = (
        df.groupBy("region")
        .count()
        .orderBy(desc("count"))
        .limit(10)
    )

    # Top 10 cities
    top_cities = (
        df.groupBy("locality_name")
        .count()
        .orderBy(desc("count"))
        .limit(10)
    )

    print(" ➤ Top 10 Regions by Number of Objects:")
    top_region.show(truncate=False)
    print(" ➤ Top 10 Cities by Number of Objects:")
    top_cities.show(truncate=False) 

    spark.stop()


# 🧠 Task 6: min_max_area_by_region
def min_max_area_by_region(**kwargs):

    spark = create_spark_session()
    df = spark.read.parquet("/opt/airflow/shared/valid_data_types.parquet")

    # min_max_area
    result = df.groupBy("region").agg(
        max("square").alias("max_square"),
        min("square").alias("min_square")
    ).orderBy("region")

    print("✅ ➤ Max & Min house square by region")
    result.show(truncate=False)

    spark.stop()


# 🧠 Task 7: buildings_by_decade
def buildings_by_decade(**kwargs):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet("/opt/airflow/shared/valid_data_types.parquet")

    # Get the year in numeric format
    df = df.withColumn("year", year(col("maintenance_year")))
    # Calculate decades
    df = df.withColumn("decade", floor(col("year") / 10) * 10)
    
    result = df.groupBy("decade").agg(count("*").alias("buildings_count")).orderBy("decade")

    # df.select("maintenance_year").distinct().orderBy("maintenance_year").show(50, False)

    print("✅ ➤ Buildings count by decade:")
    result.show(25, truncate=False)

    spark.stop()
    

# batch_loading_logic
def batch_loading_logic(df, columns, clickhouse_config, batch_size=2000, truncate_if_exists=True):
    # prepare DataFrame (parquet) to clickhouse
    def prepare_data_clickhouse(df, columns):
        rows = df.select(columns).collect()

        data = []
        for row in rows:
            cleaned_row = []
            for idx, item in enumerate(row):
                if item is None:
                    cleaned_row.append(None)
                elif isinstance(item, (datetime, date)):
                    cleaned_row.append(item)
                elif isinstance(item, str):
        
                    try:
                        parsed = datetime.strptime(item, "%Y-%m-%d").date()
                        cleaned_row.append(parsed)
                    except ValueError:
                        cleaned_row.append(item)
                else:
                    cleaned_row.append(item)
            data.append(tuple(cleaned_row))
        return data
    
    client = Client(
        host=clickhouse_config['host'],
        user=clickhouse_config['user'],
        password=clickhouse_config['password'],
        database=clickhouse_config['database']
    )

    table = clickhouse_config['table']
    
    if truncate_if_exists:
        row_count = client.execute(f"SELECT count() FROM {table}")[0][0]
        print(f"Current total rows in '{table}': {row_count}")
        
        if row_count > 0:
            print(f"Truncating table '{table}' before insert...")
            client.execute(f"ALTER TABLE {table} DELETE WHERE 1=1")

    insert_query = f"INSERT INTO {clickhouse_config['table']} ({', '.join(columns)}) VALUES"
    data = prepare_data_clickhouse(df, columns)

    total_batches = (len(data) + batch_size - 1) // batch_size
    print(f"✅ Total batches to insert: {total_batches}, batch size: {batch_size}")

    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        batch_number = i // batch_size + 1
        try:
            client.execute(insert_query, batch)
        except Exception as e:
            print(f"❌ Failed to insert batch {batch_number}: {e}")

    return client     


# 🧠 Task 10: load_data_to_clickhouse
def load_valid_data_to_clickhouse():

    spark = create_spark_session()
    valid_df = spark.read.parquet("/opt/airflow/shared/valid_data_types.parquet")

    valid_df = valid_df.withColumn(
        "maintenance_year",
        when(length(col("maintenance_year")) == 4,
             concat_ws("-", col("maintenance_year"), lit("01"), lit("01")))
        .otherwise(col("maintenance_year"))
    )

    valid_df = valid_df.withColumn("maintenance_year", to_date(col("maintenance_year"), "yyyy-MM-dd"))

    columns = [
        "house_id", "latitude", "longitude", "maintenance_year", "square", "population",
        "region", "locality_name", "address", "full_address", "communal_service_id", "description"
    ]

    clickhouse_config = {
        'host': 'clickhouse',
        'user': 'airflow_user',
        'password': '',
        'database': 'default',
        'table': 'geo_estate_data'
    }

    client = batch_loading_logic(valid_df, columns, clickhouse_config)

    row_count = client.execute(f"SELECT count() FROM {clickhouse_config['table']}")[0][0]

    print(f"📊 Current total rows in '{clickhouse_config['table']}': {row_count}")


    # valid_df.printSchema()
    valid_df.show(10)
    spark.stop()


# 🧠 Task 11: Top 25 houses square ​more than 60 sq.m
def get_top_houses(**kwargs):
    client = Client(
        host='clickhouse',
        user='airflow_user',
        password='',
        database='default'
    )
    
    query = """
        SELECT house_id, square, maintenance_year, region
        FROM geo_estate_data
        WHERE square > 60
        ORDER BY square DESC
        LIMIT 25
    """

    result = client.execute(query)
    headers = ["house_id", "square", "maintenance_year", "region"]
    table = tabulate(result, headers=headers, tablefmt="pretty")

    logging.info("\n🏠 Top 25 houses with square > 60:\n" + table)
    return result


# DAG structure
with DAG(
    dag_id='main',
    default_args=default_args,
    description='Load and Analyze CSV Data using PySpark in Airflow',
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Task 1
    load_csv_task = PythonOperator(
        task_id='load_csv_and_count_rows',
        python_callable=load_csv_to_dataframe  
    )

    # Task 2
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_dataframe  
    )

    # Task 3
    cast_columns = PythonOperator(
        task_id='convert_data_types',
        python_callable=convert_data_types  
    )

    # Task 4
    average_median_year = PythonOperator(
        task_id='average_median_year',
        python_callable=building_year_stats  
    )

    # Task 5
    top_regions_cities = PythonOperator(
        task_id='top_regions_cities',
        python_callable=top_regions_and_cities  
    )

    # Task 6
    min_max_square = PythonOperator(
        task_id='min_max_square',
        python_callable=min_max_area_by_region  
    )

    # Task 7
    calc_decade_build = PythonOperator(
        task_id='calc_decade_build',
        python_callable=buildings_by_decade  
    )

    # Task 8: Create Airflow connection to ClickHouse 
    create_conn_clickhouse = PythonOperator(
        task_id='create_conn_clickhouse',
        python_callable=create_clickhouse_connection,
    )

    # Task 9: Create ClickHouse user and table
    create_user_and_table = PythonOperator(
        task_id='create_user_and_table',
        python_callable=create_user_and_table_clickhouse,
    )

    # Task 10: load_data_to_clickhouse
    load_data_to_clickhouse = PythonOperator(
        task_id='load_data_to_clickhouse',
        python_callable=load_valid_data_to_clickhouse,
    )

    # Task 11: Top 25 houses square ​more than 60 sq.m
    top_25_house_square_60 = PythonOperator(
        task_id='top_25_house_square_60',
        python_callable=get_top_houses,
    )


    load_csv_task >> validate_data >> cast_columns >> average_median_year >> top_regions_cities >> min_max_square >> calc_decade_build >> create_conn_clickhouse >> create_user_and_table >> load_data_to_clickhouse >> top_25_house_square_60