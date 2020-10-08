import logging
import os
import io
from ftplib import FTP
from libs.ftp_utils import chdir
import datetime
import re


def execute(log: logging, config: dict):
    params = config['params']
    with FTP(params['ftp_host']) as ftp:
        ts: datetime.datetime = config['ts']
        ts_from = config['ts_from']
        ts_to = config['ts_to']
        log_types = params['log_types']
        for log_type in log_types:
            log.info("connect success")
            ftp.login(params['ftp_user'], params['ftp_password'])
            log.info("auth success")
            in_path = ts.strftime(params['in_dir'].format(log_type))
            out_path = ts.strftime(params['out_dir'].format(log_type))
            chdir(ftp, in_path)
            files = ftp.nlst()
            bio = io.BytesIO()
            for in_file in files:
                s = re.search(r"(\d+)", in_file)
                if s:
                    cur = datetime.datetime.strptime(s.group(), '%H%M%S').replace(
                        day=ts.day, month=ts.month, year=ts.year)
                    if (cur < ts_to) and (cur >= ts_from):
                        ftp.retrbinary('RETR {0}'.format(in_file), bio.write)
                        bio.write(b'\n')
            file_name = ts.strftime(params['in_file_template'])
            os.makedirs(out_path, exist_ok=True)
            out_file = os.path.join(out_path, file_name)
            with open(out_file, 'wb') as f:
                f.write(bio.getvalue())


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    conf = {
        "ts": datetime.datetime(2020, 1, 1, 0, 0),
        "ts_from": datetime.datetime(2020, 1, 1, 0, 0),
        "ts_to": datetime.datetime(2020, 1, 1, 1, 0),
        "params": {
            "ftp_host": "ftp.dlptest.com",
            "ftp_user": "dlpuser@dlptest.com",
            "ftp_password": "eUj8GeW55SvYaswqUyDSm5v6N",
            "in_dir": "data/{0}/%Y-%m-%d",
            "out_dir": "/tmp/{0}/%Y-%m-%d",
            "in_file_template": "%H%M%S.csv",
            "out_file_template": "%H%M%S.csv",
            "log_types": ["call_histories", ]
        }
    }
    execute(logger, conf)

