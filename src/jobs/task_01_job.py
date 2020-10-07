import logging

from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F, types as T


def __find_actual_active_date(lst_ad, lst_dd):
    ad = lst_ad[-1]
    for i in range(1, len(lst_dd)):
        _ad = lst_ad[i-1]
        _dd = lst_dd[i]
        if _ad != _dd:
            return _ad
    return ad


udf_actual_active_date = F.udf(
    __find_actual_active_date,
    T.StringType()
)


def execute(spark: SparkSession, log: logging, config: dict):
    log.info("extract")
    in_path = config['params']['in_path']
    out_path = config['params']['out_path']
    df = spark.read.csv(in_path, header=True).repartition(120, "PHONE_NUMBER").na.fill(
        {'DEACTIVATION_DATE': '9999-12-31'})

    log.info("transform")
    df_norm = df.sort(df.DEACTIVATION_DATE.desc()).groupby(
        ['PHONE_NUMBER']
    ).agg(
        F.collect_list(df['ACTIVATION_DATE']).alias('ACTIVATION_DATE'),
        F.collect_list(df['DEACTIVATION_DATE']).alias('DEACTIVATION_DATE')
    ).withColumn(
        'ACTUAL_ACTIVE_DATE',
        udf_actual_active_date(F.col('ACTIVATION_DATE'), F.col('DEACTIVATION_DATE'))
    ).select(['PHONE_NUMBER', 'ACTUAL_ACTIVE_DATE']).withColumn(
        "TS", F.date_format(F.date_trunc("month", "ACTUAL_ACTIVE_DATE"), "yyyy-MM"))

    log.info("load")
    df_norm.write.partitionBy("TS").parquet(out_path, mode="overwrite")
    spark.read.parquet(out_path)
