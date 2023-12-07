# import ETL scripts 
from scripts.s3_extract import extract
from scripts.transform import transform
from scripts.load import load
import logging
from datetime import datetime

job_timestamp = datetime.now().replace(microsecond=0)
job_timestamp = datetime(2011, 1, 3, 0, 0, 0)

logging.basicConfig(filename=f'./logs/log_{job_timestamp}.log', encoding='utf-8', level=logging.WARNING)

extract(job_timestamp)
transform(job_timestamp)
load(job_timestamp)