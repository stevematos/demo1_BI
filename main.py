import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import argparse

from etl import ETL

def main():

	parser = argparse.ArgumentParser()

	parser.add_argument('-i','--input',
						help='Input for the pipeline',
						default='./amarillo.csv')
	parser.add_argument('-o','--output',
						help='Output for the pipeline',
						default='./players-final.txt')

	args = parser.parse_args()

	print args


	extract_file_csv = args.input

	query_database = """
			select * from players
	"""

	config_database = {
			'drivername':'postgresql',
			'host':'35.247.199.205',
			'port' : 5432,
			'username' : 'postgres',
			'password' : 'secret',
			'database' : 'fifa',
			'table': 'players',
			'query': query_database
	}

	output_file = args.output

	p = beam.Pipeline(options=PipelineOptions())

	etl = ETL(p)

	etl.process(extract_file_csv,config_database,output_file)

	print extract_file_csv

if __name__ == "__main__":
	main()
