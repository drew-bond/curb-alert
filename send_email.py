import os
import certifi
import pandas as pd
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


with open('secrets.json') as f:
    secrets = json.load(f)
apikey = secrets['sendgrid_api_key']
from_email = secrets['from_email']
to_emails = secrets['to_emails']


def send_email_if_condition_met(df, col1, col2, caption_col):
    """
    Sends an email if any row in the dataframe has a True value in one of the two specified columns.
    
    Parameters:
    - df: The dataframe to check.
    - col1: The first column name to check for a True value.
    - col2: The second column name to check for a True value.
    - caption_col: The column name containing the caption to include in the email body.
    """
    email_counter = 0

    for index, row in df.iterrows():
        if row[col1] == True or row[col2] == True:
            message = Mail(
                from_email=from_email,
                to_emails=to_emails,
                subject='There is a new curb alert near you!',
                html_content=f'<strong>{row[caption_col]}</strong>'
            )
            try:
                sg = SendGridAPIClient(apikey)
                response = sg.send(message)
                if response.status_code == 202:
                    email_counter += 1
                #print(response.status_code)
                #print(response.body)
                #print(response.headers)
            except Exception as e:
                print(e)
    print(f"Emails sent: {email_counter}")

#df_test = pd.read_csv('proximity_checked_test.csv')

#send_email_if_condition_met(df_test, 'regex_nearby', 'parsed_nearby', 'Caption')

