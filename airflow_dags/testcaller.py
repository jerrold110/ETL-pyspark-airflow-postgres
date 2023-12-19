# import ETL scripts 
from etl_scripts.s3_extract import extract
from etl_scripts.transform import transform
from etl_scripts.load import load
from etl_scripts.date_filepath import date_filepath
import logging
from datetime import datetime

job_timestamp = datetime.now().replace(microsecond=0)
job_timestamp = datetime(2011, 1, 3, 0, 0, 0)
job_timestamp = date_filepath(job_timestamp)

logging.basicConfig(filename=f'./logs/log_{job_timestamp}.log', encoding='utf-8', level=logging.WARNING)

extract(job_timestamp)
transform(job_timestamp)
load(job_timestamp)