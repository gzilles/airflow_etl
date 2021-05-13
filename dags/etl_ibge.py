# External librarys
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import timedelta, datetime

# DAG default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    "start_date": datetime(2021, 5, 12, 00, 00),
    'email': ['data_engineer@vamosjuntos.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 24,
    'retry_delay': timedelta(hours=1),
    'execution_timeout': timedelta(hours=1),
    }

# DAG instance and tasks with decorators
@dag(default_args=default_args, schedule_interval=None, description='ETL para extrair dados IBGE para Datalake e DW')
def etl_ibge():
	
    @task
    def extract_from_api_to_landing_zone(param):

        # Internal librarys
        import boto3
        import requests
        import json
        from datetime import datetime

        # AWS credentials
        aws_access_key_id = Variable.get('aws_access_key_id')
        aws_secret_access_key = Variable.get('aws_secret_access_key')


        # Connect to IBGE API URL to extract data and upload json file to S3 landing zone returning S3 key file
        url = 'https://servicodados.ibge.gov.br/api/v1/' + param + '/distritos'
        response = requests.get(url).json()
        iso_date = datetime.now().isoformat()
        key_file = f'raw/ibge/{param}-json-list-{iso_date[:-7]}.json'
        bucket = 'vamos-juntos-landing-us-est-1'
        s3 = boto3.resource('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
        obj = s3.Object(bucket, key_file)
        obj.put(Body=json.dumps(response))

        return key_file

    
    @task
    def extract_from_mongo_to_landing_zone(param):
      
        # Internal librarys
        import json
        from datetime import datetime
        import pymongo
        import boto3
        
        # AWS credentials
        aws_access_key_id = Variable.get('aws_access_key_id')
        aws_secret_access_key = Variable.get('aws_secret_access_key')
        
        # Mongo DB credentials
        mongo_user = Variable.get('mongo_user')
        mongo_password = Variable.get('mongo_password')
        mongo_host = 'unicluster.ixhvw.mongodb.net'
        mongo_db = 'ibge'
        uri = f'mongodb+srv://{mongo_user}:{mongo_password}@{mongo_host}/{mongo_db}?retryWrites=true&w=majority'
        
        # Connect to MongoDB to extract data and upload json file to S3 landing zone returning S3 key file
        client = pymongo.MongoClient(uri)
        db = client.ibge
        pnad_collec = db.pnadc20203
        response = list(pnad_collec.find({}, {'_id': 0}))
        iso_date = datetime.now().isoformat()
        key_file = f'raw/ibge/{param}-{iso_date[:-7]}.json'
        bucket = 'vamos-juntos-landing-us-est-1'
        
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        obj = s3.Object(bucket, key_file)
        obj.put(Body=json.dumps(response))

        return key_file

    
    @task
    def upload_mongo_parquet_to_transformed_zone(file_name):

        # Internal librarys
        import pandas as pd
        import boto3
        import json
        from datetime import datetime
        from io import BytesIO
        
        # AWS credentials
        aws_access_key_id = Variable.get('aws_access_key_id')
        aws_secret_access_key = Variable.get('aws_secret_access_key')
        
        # Load data from S3 landing zone
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket = 'vamos-juntos-landing-us-est-1'
        obj = s3.Object(bucket, file_name)
        jsons = json.loads(obj.get()['Body'].read())
        
        # Transform data frame to parquet and create buffer object 
        data =  [{'ano': json['ano'], 
               'anosesco': json['anosesco'], 
               'cor': json['cor'], 
               'graduacao:': json['graduacao'],
               'horastrab': json['horastrab'],
               'idade': json['idade'],
               'ocup': json['ocup'],
               'renda': json['renda'],
               'sexo': json['sexo'],
               'trab': json['trab'],
               'trimestre': json['trimestre'],
               'uf': json['uf']                  
        } for json in jsons]

        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        
        # Upload parquet file to S3 transnformed zone returning key file
        iso_date = datetime.now().isoformat()
        parquet_file_name = f'parquet/ibge/localidades-{iso_date[:-7]}.parquet'
        bucket = 'vamos-juntos-transforming-us-est-1'
        obj = s3.Object(bucket, key_file)
        buffer.seek(0)
        obj.put(Body=buffer.getvalue())

        return key_file

    
    @task
    def upload_api_parquet_to_transformed_zone(key_file):

        # Internal librarys
        import pandas as pd
        import boto3
        import json
        from datetime import datetime
        from io import BytesIO
        
        # AWS credentials
        aws_access_key_id = Variable.get('aws_access_key_id')
        aws_secret_access_key = Variable.get('aws_secret_access_key')
        
        # Load data from S3 landing zone
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket = 'vamos-juntos-landing-us-est-1'
        obj = s3.Object(bucket, key_file)
        jsons = json.loads(obj.get()['Body'].read())
        
        # Transform data frame to parquet and create buffer object
        data = [{'id': json['id'], 
            'nome': json['nome'], 
            'municipio_id': json['municipio']['id'], 
            'municipio_nome': json['municipio']['nome'], 
            'microrregiao_id': json['municipio']['microrregiao']['id'], 
            'microrregiao_nome': json['municipio']['microrregiao']['nome'], 
            'mesorregiao_id': json['municipio']['microrregiao']['mesorregiao']['id'], 
            'mesorregiao_nome': json['municipio']['microrregiao']['mesorregiao']['nome'], 
            'regiao_imediata_id': json['municipio']['regiao-imediata']['id'], 
            'regiao_imediata_nome': json['municipio']['regiao-imediata']['nome'], 
            'regiao_intermediaria_id': json['municipio']['regiao-imediata']['regiao-intermediaria']['id'], 
            'regiao_intermediaria_nome': json['municipio']['regiao-imediata']['regiao-intermediaria']['nome'], 
            'uf_id': json['municipio']['microrregiao']['mesorregiao']['UF']['id'], 
            'uf_sigla': json['municipio']['microrregiao']['mesorregiao']['UF']['sigla'],
            'uf_nome': json['municipio']['microrregiao']['mesorregiao']['UF']['nome'], 
            'regiao_id': json['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id'], 
            'regiao_sigla': json['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['sigla'], 
            'regiao_nome': json['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome']
        } for json in jsons]

        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        
        # Upload parquet file to S3 transnformed zone returning key file
        iso_date = datetime.now().isoformat()
        parquet_file_name = f'parquet/ibge/localidades-{iso_date[:-7]}.parquet'
        bucket = 'vamos-juntos-transforming-us-est-1'
        obj = s3.Object(bucket, key_file)
        buffer.seek(0)
        obj.put(Body=buffer.getvalue())

        return key_file

        
    @task
    def ingest_to_mysql(key_file, param):
        
        # Internal librarys
        import pymysql
        import pandas as pd
        from sqlalchemy import create_engine
        from sqlalchemy_utils import database_exists, create_database
        import boto3
        import pyarrow.parquet as pq
        from io import BytesIO
        from datetime import datetime
        
        # AWS credentials
        aws_access_key_id = Variable.get('aws_access_key_id')
        aws_secret_access_key = Variable.get('aws_secret_access_key')

        # Load data from S3 transformed zone
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket = 'vamos-juntos-transforming-us-est-1'
        buffer = BytesIO()
        obj = s3.Object(bucket, key_file)
        obj.download_fileobj(buffer)
        table = pq.read_table(buffer)
        df = table.to_pandas()
        df['created_at'] = datetime.today()

        # MySQL credentials
        mysql_user =  Variable.get('mysql_user')
        mysql_password = Variable.get('mysql_password')
        host = 'vamos-juntos-dw.croygqtawxvm.us-east-1.rds.amazonaws.com'
        db = 'ibge'
        uri = f'mysql+pymysql://{mysql_user}:{mysql_password}@{host}/{db}?charset=utf8mb4'

        # Create MySQL connection and create database if not exists
        engine = create_engine(uri)

        if not database_exists(engine.url):
            create_database(engine.url)
        else:
            engine.connect()

        # Ingest data into MySQL
        df.to_sql(param, con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)


    # API tasks dependencies
    api_landing_file_name = extract_from_api_to_landing_zone('localidades')
    api_parquet_file_name = upload_api_parquet_to_transformed_zone(api_landing_file_name)
    ingest_to_mysql(api_parquet_file_name, 'localidades')
        
    # MongoDB tasks dependecies
    mongo_landing_file_name = extract_from_mongo_to_landing_zone('pnda20023')
    mongo_parquet_file_name = upload_mongo_parquet_to_transformed_zone(mongo_landing_file_name)
    ingest_to_mysql(mongo_parquet_file_name, 'pnda20023')

# Start DAG instance
dag = etl_ibge()