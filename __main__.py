import os
from io import StringIO
import logging
from modules import DataConn
from dotenv import load_dotenv
import requests
import pandas as pd
from sqlalchemy import create_engine

url = "https://api.adviceslip.com/advice"

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::MainModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

load_dotenv()

def read_api():
    try:
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data)
            return df
    
        else:
            print(f"Error en la solicitud: {response.status_code}")
    
    except Exception as e:
        print("Error al leer los datos..")
        print(e)

def main():
    user_credentials = {
        "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
        "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
        "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
        "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
        "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME')
    }

    


    schema:str = "francomariano147_coderhouse"
    table:str = "advice_table"

    data_conn = DataConn(user_credentials, schema)
  #  data_retriever = DataRetriever()

    try:
        predata = read_api()
        data = predata.astype({'slip.id': str, 'slip.advice': str})
        print(data.info)
        
        ##print(data.values[1])
       
        conn = create_engine(f"postgresql://{user_credentials['REDSHIFT_USERNAME']}:{user_credentials['REDSHIFT_PASSWORD']}@{user_credentials['REDSHIFT_HOST']}:/{user_credentials['REDSHIFT_PORT']}/{user_credentials['REDSHIFT_DBNAME']}")
    
       # data.to_sql('francomariano147_coderhouse.advice_table', conn, index=False, if_exists='replace')
        
        data_conn.upload_data(data, 'advice_table')
        logging.info(f"Data uploaded to -> {schema}.{table}")
        print(f"Succefully Data uploaded to -> {schema}.{table}")

    except Exception as e:
        logging.error(f"Not able to upload data\n{e}")
        print(f"ERROR: Not able to upload data\n{e}")
        
    finally:
        data_conn.close_conn()

if __name__ == "__main__":
    main()