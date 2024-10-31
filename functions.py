import pandas as pd
import requests
import os

def extract_data() -> pd.DataFrame:
    #filmData = pd.read_csv(r'C:\Users\nicke\Desktop\Reps\BD_Nick\data.csv')
    url = 'http://127.0.0.1:8000/get_data'
    responds = requests.get(url)
    filmData = responds.json()
    filmData_df = pd.DataFrame(filmData)


    return filmData_df

def load_data(transformed_data: pd.DataFrame) -> str:
    if os.path.exists(r'C:\Users\nicke\Desktop\Reps\BD_Nick\transformed_data.csv'):
        print("Data already exists. Skipping load.")
        pass
    else:
        transformed_data.to_csv(r'C:\Users\nicke\Desktop\Reps\BD_Nick\transformed_data.csv', index=False)
    return r'C:\Users\nicke\Desktop\Reps\BD_Nick\transformed_data.csv'

def transform_data(filmLoco: str) -> pd.DataFrame:
    data_df = pd.read_csv(filmLoco)

    transformed_data = data_df[data_df['averageRating'] >= 9.0]
    return transformed_data

def startExection() -> pd.DataFrame:
    film_data = extract_data()
    filmLoco = load_data(film_data)
    transformed_data = transform_data(filmLoco)
    return transformed_data