import pandas as pd
import logging
import functions
from hamilton import driver
from hamilton_sdk import adapters
from fastapi import FastAPI
...
tracker = adapters.HamiltonTracker(
   project_id=1,  # modify this as needed
   username="Nick",
   dag_name="BD_DAG",
   tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"},
)
dr = (
    driver.Builder()
    .with_modules(functions)
    .with_adapters(tracker)
    .build()
)

app = FastAPI()

@app.get("/get_data")
async def get_data():
    try:
        film_data = pd.read_csv(r'C:\Users\nicke\Desktop\Reps\BD_Nick\data.csv')
        return film_data.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error in get_data: {e}")
        return {"error": str(e)}

def pipeline():
    try:
        output = dr.execute(['startExection'])
        transformed_data = output['startExection']
        return transformed_data
    except Exception as e:
        logging.error(f"Error in pipeline: {e}")
        return None

def run():

    transformed_data = pipeline()
        
if __name__ == "__main__":
    run()    

