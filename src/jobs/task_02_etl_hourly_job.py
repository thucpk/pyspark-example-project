import logging
import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from libs.postgres_utils import get_postgres_cli


def execute(spark: SparkSession, log: logging, config: dict):
    log.info("extract")
    params = config['params']
    ps_conf = config['postgres']

    ts: datetime.datetime = params['ts']
    in_path = ts.strftime(params['in_path'])
    ts_from = config['ts_from']
    ts_to = config['ts_to']
    df = spark.read.csv(in_path, header=True, sep=';')
    df.select(
        F.col('FROM_PHONE_NUMBER'), F.col('TO_PHONE_NUMBER'),
        F.to_timestamp(df['START_TIME'], 'dd/MM/yyyy HH:mm:ss').alias('START_TIME'),
        F.col('CALL_DURATION').cast('long'), F.col('IMEI'), F.col('LOCATION')
    ).withColumn("TS", F.date_format(F.date_trunc("hour", "START_TIME"), "yyyy-MM-dd-HH"))
    df.write.partitionBy("TS").mode('append').format('hive').saveAsTable('task_02')
    df = spark.sql("select * from task_02 where TS >= {} AND TS < {}".format(ts_from, ts_to)).drop_duplicates()
    df.cache()
    ts = df.select("TS").rdd.map(lambda x: x[0]).first()
    # Number of call, total call duration.
    num_call = df.count()
    total_call_duration = list(df.select(F.sum(df['CALL_DURATION'])).first().asDict().values())[0]

    # Number of call in working hour (8am to 5pm)
    num_call_working_hour = df.filter("hour(START_TIME) >= 8 AND hour(START_TIME) <= 17").count()

    # Find the IMEI which make most call.
    imei_most = df.groupBy('IMEI').count().sort(F.col("count").desc()).first().asDict()

    # Find top 2 locations which make most call.
    locations = list(map(lambda x: x.asDict(), df.groupBy('LOCATION').count().sort(F.col("count").desc()).head(2)))

    rs = (ts, num_call, total_call_duration, num_call_working_hour, imei_most, locations)
    with get_postgres_cli(ps_conf) as ps_cli:
        with ps_cli.cursor() as cur:
            sql = """
            INSERT INTO metric_hour(
                ts, num_call, total_call_duration, 
                num_call_working_hour, imei_most, locations
            ) VALUES(%s, %s, %s, %s, %s, %s) 
            ON CONFLICT (ts) 
            DO UPDATE SET(
                num_call, total_call_duration, num_call_working_hour, imei_most, locations) = 
                (EXCLUDED.num_call, EXCLUDED.total_call_duration, EXCLUDED.num_call_working_hour
                 EXCLUDED.imei_most, EXCLUDED.locations)
            """
            cur.execute(sql, rs)
