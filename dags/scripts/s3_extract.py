
def extract(job_timestamp):
	"""
	Extraction job. Ensure that Aws cli configuration with IAM is already done to resolve authentication issues
	"""
	import os
	import logging
	import boto3
	import botocore

	print('Extract starting')
	# Create the destination folder
	isExist = os.path.exists(f'./extracted_data/{job_timestamp}')
	if not isExist:
		os.makedirs(f'./extracted_data/{job_timestamp}')

	# Downloads Customers.csv to path relative to the current working directory
	s3 = boto3.resource('s3')

	def download(key):
		s3.Object(bucket_name='test-project-j2400-2', key=f'dvd/{key}')\
		.download_file(f'./extracted_data/{job_timestamp}/{key}')

	objects = ['address.dat','category.dat','city.dat','country.dat','customer.dat','film.dat','film_category.dat','inventory.dat',
			'language.dat','payment.dat','rental.dat','staff.dat','store.dat']
	
	for o in objects:
		try:
			logging.error(f'Retrieving {o} from S3')
			download(o)
		except botocore.exceptions.ClientError as e:
			logging.error(f'Error while retrieving {o} from S3 {e}')
		except Exception as e:
			logging.error(f'Error while retrieving {o} from S3 {e}')

	print('Extract operation complete')

if __name__ == '__main__':
	from datetime import datetime
	extract(datetime(2011, 1, 1, 0, 0, 0))
