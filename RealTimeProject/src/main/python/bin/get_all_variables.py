import os


### Set environment variables
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get environment variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

### Set other variables
appName = 'USA Prescriber Research Report'
current_path = os.getcwd()
staging_dim_city = current_path + '/../staging/dimension_city'
staging_fact = current_path + '/../staging/fact'
staging_dim_city_prod  = 'PrescPipeline/staging/dimension_city'
staging_dim_fact_prod  = 'PrescPipeline/staging/fact'
output_city='PrescPipeline/output/dimension_city'
output_fact='PrescPipeline/output/presc'

