# airflow_etl

Esse é o meu projeto final de conclusão do Bootcamp de Engenharia de Dados IGTI 2021/1.

O objetivo desse projeto é trabalhar através de exercícios práticos os conceitos de:

- Pipeline de Dados
- Containers
- Base de Dados SLQ e NoSQL
- Conexão com APIs
- ETL
- Data Lake
- Processamento de dados distribuído

O projeto consiste em construir um pipeline de dados para a empresa Vamos Juntos que faça a extração dos dados em uma instância de MongoDB na nuvem e na API do site do IBGE para depois depositar no Data Lake da empresa. Após a ingestão dos dados no Data Lake, ele é tratado e filtrado para ser disponibilizado apenas para o público de interesse da empresa em um DW. Com os dados no DW, você vai realizar algumas consultas e extrair resultados importantes para a #VamosJuntos.

Foi criada uma estrutura usando Docker-compose para utilização do Apache Airflow como ferramenta de orquestração da minha ETL, pois foi é uma das ferramentas que mais domino atualmente. Caso não saiba como instalar Docker Community Edition e Docker Compose vou deixar a instruções para o Ubuntu abaixo:

1) Instalando os pacotes necessários para o Docker e configurando o repositório.

```
$ sudo apt-get update

$ sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common

$ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

$ sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
```

2) Instalando o Docker.

```
$ sudo apt-get update
$ sudo apt-get install docker-ce docker-ce-cli containerd.io
```

3) Criando um grupo para o docker e adicionando o usuário atual nele para ter permissão de execução sem uso de sudo.

```
$ sudo groupadd docker
$ sudo usermod -aG docker $USER
```

Nota: Depois de adicionar o usuário faça logoff e login novamente para validar a configuração.

4) Instalando o Docker Compose.

```
$ sudo curl -L "https://github.com/docker/compose/releases/download/1.29.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
$ sudo chmod +x /usr/local/bin/docker-compose
$ sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
```

5) Teste as instalações com os comandos.

```
$ docker version
$ docker-compose version
```

O próximo passo é instalar o Git (caso não esteja instalado) e copiar os arquivos de configuração do Github para a máquina local.

```
$ sudo apt-get install -y git
$ git clone "https://github.com/gzilles/airflow_etl.git"
```

De dentro da pasta copiada execute os comandos abaixo que baixa uma imagem do Apache Airflow que eu criei com todos os módulos de Python necessários para execução desse projeto e depois sobe os containers necessários.   

```
$ cd airflow_etl
$ docker pull gzilles/airflow
$ docker-compose -d
```

Caso você deseje fazer alguma alteração no código que necessite de algum módulo adicional, basta alterar o arquivo requirements.txt nessa mesma pasta e executar o comando abaixo para criar uma nova imagem com os módulos adicionais e subir os containers novamente.

```
$ docker build -t gzilles/airflow .
$ docker-compose -d
```

Com o comando abaixo você pode verificar os containers que foram criados.

```
$ docker ps
```

Agora você já pode acessar a interface web do Airflow.

```
http://localhot:8080
# Usuário: airflow
# Senha: airflow
```

Na aba Admin/Variable da interface web você deve configurar as variáveis sensíveis de ambiente conforme abaixo:

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/variables.JPG?raw=true)

Na console do AWS vamos criar um usuário e configurar chaves de acesso e permissões de segurança para acessar o S3 conforme abaixo que nesse caso não estão mais válidas por isso estão expostas (não é aconselhável por questões de segurança usar as credenciais do usuário root para essa finalidade). Essas chaves de acesso devem ser configuradas nas variáveis de ambiente do Airflow que vimos no intem anterior.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/access%20key.JPG?raw=true)
  
Também na console do AWS vamos criar um banco de dados RDS. Você deve utilizar a instância MySQL 8.0.20 Free Tier db.t2.micro com 20 gb para ter direito a versão gratuita. O host, o usuário e a senha criadas também devem ser configurados nas variáveis de ambiente do Airflow. Não esqueça de habilitar o acesso público e de liberar o acesso externo na porta 3306 em VPC Security Group. 

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/rds.JPG?raw=true)
 
Abaixo estão o usuário e a senha disponibilizadas para acesso da instância do MongoDB na nuvem mas não sei até quando ele vai ficar no ar. Então em breve vou adicionar uma instância do MongoDB em um container local com os dados para importação. Por enquanto essas credenciais também devem ser configuradas nas variáveis de ambiente do Airflow.

```
username: estudante_igti
password: SRwkJTDz2nA28ME9
```

No AWS S3 vamos criar 2 buckets que servirão como base do nosso Data Lake.

```
vamos-juntos-landing-us-est-1
vamos-juntos-transforming-us-est-1
```

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/buckets.JPG?raw=true)

Na pasta dags se encontra o arquivo etl_ibge.py que é a base da nossa DAG no Airflow e que será explicado abaixo:
Nota: Você pode copiar seus próprios scripts python para a pasta dag e criar suas próprias DAGs. Essa pasta local está mapeada como um volume no container do Airflow e será reconhecida na interface web em no máximo 30 segundos.

1. Essa é a estrutura básica do arquivo com os módulos externos importados, os argumentos padrões da DAG, a instância da DAG e tarefas declaradas usando a nova função de decoradores presente na versão 2 do Airflow. As tarefas foram retiradas do corpo pois serão explicadas separadamente mais adiante e por último temos as dependências das tarefas e a DAG instanciada. Sempre lembrar de setar um start_date em default_args caso contrário sua DAG não funcionará. Eu setei um e-mail aleatório para receber um e-mail em caso de falha (precisa configurar um mail server para isso) e defini que em caso de falha a tarefa seria tentada novamente a cada hora num total de 24 vezes e com timeout de 1 hora. Essas configurações não serão utilizadas apenas configurei com exemplo
```
# External librarys
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import timedelta, datetime

# DAG default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    "start_date": datetime(2021, 5, 12, 00, 00),
    # 'email': ['data_engineer@vamosjuntos.com.br'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 24,
    # 'retry_delay': timedelta(hours=1),
    # 'execution_timeout': timedelta(hours=1),
    }

# DAG instance and tasks with decorators
@dag(default_args=default_args, schedule_interval=None, description='ETL para extrair dados IBGE para Datalake e DW')
def etl_ibge():
	
    # @task 
    # extract_from_api_to_landing_zone
    
    # @task 
    # extract_from_mongo_to_landing_zone
    
    # @task 
    # upload_mongo_parquet_to_transformed_zone
    
    # @task 
    # upload_api_parquet_to_transformed_zone
    
    # @task 
    # ingest_to_mysql
        
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

```
2. Essa é a task responsável por extrair os dados no site do IBGE através da API e importar os dados crus para o primeiro estágio do nosso Data Lake. Módulos específicos são importados internamente para evitar consumo desnecessário de memória. As credenciais da AWS são importadas das variáveis do Airflow e após a solicitação os dados recebidos são salvos em formato original no S3.

```
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
```

3. Essa é a task responsável por extrair os dados do IBGE da instância online do MongoDB e importar os dados crus para o primeiro estágio do nosso Data Lake. Módulos específicos são importados internamente para evitar consumo desnecessário de memória. As credenciais do MongoDB são importadas das variáveis do Airflow e após a solicitação os dados recebidos são salvos no seu formato original no S3.

```
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
```

4. Essa task é responsável por carregar os dados crus do primeiro estágio do Data Lake, transformar a estrutura json do MongoDB para uma única dimensão de data frame e salvar como um arquivo parquet em outro estágio de transformação.

```
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
```

5. Essa task é responsável por carregar os dados crus do primeiro estágio do Data Lake, transformar a estrutura json do arquivo da API para uma única dimensão de data frame e salvar como um arquivo parquet em outro estágio de transformação.

```
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
```

6. Por último essa tarefa carrega o arquivo parquet do S3 em um data frame novamente, importa as credenciais do MySQL das variáveis do Airflow e ingere os dados no nosso DW.

```
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
```

Agora que sabemos nosso código em detalhe vamos voltar para a página inicial do Airflow onde nossa DAG aparecera conforme a imagem abaixo.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/dag.JPG?raw=true)

Para demonstração a DAG foi setada sem um intervalo de agendamento, então precisamos ligar ela deslizando o botão do lado esquerdo e rodando ela manualmente no botão de play da direita em actions (caso as configurações de agendamento no default_args estejam descomentadas o agendador irá criar e agendas as tarefas automaticamente conforme configurado). É importante lembrar que ela só iniciará se estiver ligada. As imagens abaixo demonstram como as tarefas são agendadas e executadas.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/running_dag.JPG?raw=true)

Depois de ativada podemos acompanhar a execução das tarefas em modo árvore.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/tree_view.JPG?raw=true)

Podemos visualizar também em tempo real no modo gráfico.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/graph_view.JPG?raw=true)

E e verificar a duração da DAG e suas tarefas.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/gantt.JPG?raw=true)

Agora ao acessar a nossa landing zone podemos verificar os arquivos json salvos.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/landing_zone.JPG?raw=true)

Depois na nossa transforming zone estão os arquivos parquet compactados.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/transforming_zone.JPG?raw=true)

E finalmente verificamos que a tabela localidades foi criada e está com dados inseridos no MySQL.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/localidades.jpg?raw=true)

A mesma coisa aconteceu com a tabela pnad20023.

![alt text](https://github.com/gzilles/airflow_etl/blob/main/images/pnad20023.jpg?raw=true)

Finalmente estamos prontos para realizar as pesquisas e correlações necessárias para atingir os resultados esperados pelo cliente.

Essa é a primeira versão publicada, já foram feitas várias melhorias no código como trabalhar com os arquivos apenas em memória sem salvar localmente. Meu próximo passo é transformar essas tarefas em funções Lambda do AWS e usar o Airflow apenas para orquestrar e disparar através do Boto3a s Lambdas tirando todo processamento local da máquina. Sei que ainda tem muita coisa a melhorar ainda e fico aberto a opiniões, sugestões e críticas construtivas de todos.

[Gustavo Zilles](mailto:gzilles@hotmail.com)
