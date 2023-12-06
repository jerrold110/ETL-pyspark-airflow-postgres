# import ETL scripts 
from scripts.s3_extract import extract
from scripts.transform import transform

from datetime import datetime

job_timestamp = datetime.now().replace(microsecond=0)
job_timestamp = datetime(2011, 1, 1, 0, 0, 0)

extract(job_timestamp)
transform(job_timestamp)
load(job_timestamp)