import pandas as pd
import requests
from datetime import datetime

def load_links(PATH_and_NAME:str)-> pd.DataFrame:
    return pd.read_csv(PATH_and_NAME)


def check_link_health(url):
    try:
        response = requests.get(url,timeout=10)  
        return response.status_code 
    except requests.ConnectionError:
       return "Not healthy: Connection error"
    except requests.Timeout:
       return "Not healthy: timeout error"
    except requests.RequestException as e:
        return "Not healthy: An error occurred: {e}"
    except:
        return "Not healthy"
    
def add_health_column(df:pd.DataFrame)-> pd.DataFrame:
    df['Link health'] = df['link'].apply(check_link_health)
    return df

def add_date_column(df:pd.DataFrame)-> pd.DataFrame:
    todays_date = datetime.today().strftime("%Y_%m_%d")
    df['Date'] = todays_date
    return df 