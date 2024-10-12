import pandas as pd
import requests
from datetime import datetime
import boto3_funcs

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

def append_to_link_repo_then_check_for_duplicates(client,
                                                  new_batch_df:pd.DataFrame,
                                                 todays_date:str )-> pd.DataFrame:
   repo_df = pd.read_csv(boto3_funcs.get_file(client=client,
                                              file_name="/monitoring/linkrepo",
                                              object_name="/monitoring/linkrepo"))
   
   new_temp_df = pd.concat(repo_df,new_batch_df)
   boto3_funcs.upload_file(client=client,
                           object_name="/monitoring/linkrepo",
                           file_name="/monitoring/linkrepo")
   new_temp_df['is_duplicates'] = new_temp_df.duplicated(subset=['link'], keep=False).astype(int)

   final_df = new_batch_df[(new_batch_df["Date"]==todays_date)]
   return final_df
