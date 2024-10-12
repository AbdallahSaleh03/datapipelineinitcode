#!/usr/bin/env python3

import boto3
import pandas as pd
import numpy as np
import re
import emoji
import openpyxl
import argparse
from datetime import datetime

def is_arabic(char):
    # Check if the character is within the Arabic Unicode block
    return '\u0600' <= char <= '\u06FF'

def clean_text(text, min_length=1):
    # Filter characters: keep Arabic letters, emojis, or spaces
    filtered_text = ''.join([char for char in text if is_arabic(char) or char == ' ' or emoji.is_emoji(char)])

    # Remove short sentences (less than min_length)
    filtered_text = ' '.join([sentence for sentence in filtered_text.split('.') if len(sentence.split()) >= min_length])

    # Remove empty strings and leading/trailing whitespaces
    filtered_text = ' '.join([word for word in filtered_text.split() if word != '']).strip()

    # If the text consists only of emojis, return an empty string
    if all(emoji.is_emoji(char) for char in filtered_text) and len(filtered_text) > 0:
        return ''

    return filtered_text

def cleaning_metadata(PATH:str = 'facebook-comments66f54eb456b31-pfbid0TdSoQi1hPD4YLJEP7qD78cempMtTBc8Zk8nqGsZJztRgRMfcSipF1bzkMy6K43C6l.xlsx'):
    # Loading the Dataset
    df = pd.read_excel(f'{PATH}', skiprows = 6)

    # Dropping Unnecessary Columns
    df = df.drop(['Unnamed: 0', 'Unnamed: 1', 'Name (click to view profile)', 'Profile ID', 'Date','Likes', 'Live video timestamp'], axis=1)

    # Cleaning the Comment Column ; Applying the ```clean_text``` function to each comment in the ```Comment``` column of the dataframe
    df['Comment'] = df['Comment'].apply(clean_text)

    # Drop rows where the 'comments' column is empty
    df = df[df['Comment'] != '']
    # Exporting to the Desired Format
    df.to_excel('cleaned_data.xlsx', index=False)

def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--path",
                        help="Path to excel file")
    args = parser.parse_args()

    if args.path == None:
        cleaning_metadata()
    else:
        cleaning_metadata(args.path)


def create_subfolder(data_type_folder):
    todays_date = datetime.today().strftime("%Y_%m_%d")
    s3 = boto3.client('s3')
    s3.put_object(
    Bucket='bucket-name',
    Key=f'{data_type_folder}/{todays_date}/type.csv',
    
)
    
if __name__ == "__main__":
    main()
