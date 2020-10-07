import argparse
import importlib
import datetime
from dateutil.parser import parse as parse_time

from libs.spark import start_spark
from libs.app import start_app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help="The name of the job module you want to run")
    parser.add_argument('--config', type=str, dest='job_config',
                        help="Extra job config")
    parser.add_argument('--ts', type=parse_time, default=datetime.datetime.now(), dest='ts',
                        help="Execution date")
    parser.add_argument('--from_ts', type=parse_time, default=None, dest='from_ts',
                        help="Execution from date")
    parser.add_argument('--to_ts', type=parse_time, default=None, dest='to_ts',
                        help="Execution to date")
    parser.add_argument('--ignore_spark', action='store_true')

    args = parser.parse_args()
    job_name = args.job_name
    job_file_conf = args.job_config if args.job_config else "configs/{0}.json".format(job_name)
    print(args)

    if args.ignore_spark:
        config = start_app(app_name=job_name, config_file=job_file_conf)
        # Execution date
        config['ts'] = args.ts
        config['from_ts'] = args.from_ts
        config['to_ts'] = args.to_ts

        job_module = importlib.import_module('jobs.%s' % args.job_name)
        job_module.execute(config)
    else:
        spark, log, config = start_spark(
            app_name=job_name,
            files=[job_file_conf])

        # Execution date
        config['ts'] = args.ts
        config['from_ts'] = args.from_ts
        config['to_ts'] = args.to_ts

        # log that main ETL job is starting
        log.info('SPARK :: {0} :: is up-and-running'.format(job_name))
        job_module = importlib.import_module('jobs.%s' % args.job_name)
        job_module.execute(spark, log, config)
        log.info('SPARK :: {0} :: complete'.format(job_name))
