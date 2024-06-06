import requests
import pandas as pd

url = "https://api.adviceslip.com/advice"


# URL de la API de REST Countries
url2 = "https://restcountries.com/v3.1/all"

# Realizar la solicitud GET a la API
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    df = pd.json_normalize(data)
    print(df.dtypes)
    print(df)
else:
    print(f"Error en la solicitud: {response.status_code}")
