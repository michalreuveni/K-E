#!/usr/bin/python3.7 -u
import requests
import json
import pandas as pd
import base64
import pathlib
import numpy as np
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail



def create_list():
    cidlist = []
    url = "https://metadataservice.bc2.customers.biocatch.com/prod/customers/?apikey=b%40%2BXju9Cwj%25r%26X8TwX%24%5EK%5E-uXXbd%25Z%24N"
    response = requests.get(url)
    json_data = json.loads(response.text)
    for key, value in json_data.items():
        if 'api_request_params' in value:
            cidlist.append(key)
    return cidlist


def get_todays_results(df, cid_array):
    row_num = 0

    for i in cid_array:
        url = "https://metadataservice.bc2.customers.biocatch.com/prod/customers?cids=" + i + "&apikey=b%40%2BXju9Cwj%25r%26X8TwX%24%5EK%5E-uXXbd%25Z%24N"
        response = requests.get(url)
        json_data = json.loads(response.text)
        data_to_add = json_data[i]['api_request_params']
        row_num = row_num + 1
        df.loc[row_num, 'cid_list'] = i
        df.loc[row_num, 'todays_results'] = data_to_add

    return df




def main():
    # creating path for first time
    path = str(pathlib.Path().parent.resolve()) + "/API_CMDS/"


    # create CID list
    cid_list = create_list()
    cid_array = np.array(cid_list)
    cid_array = cid_array.tolist()

    # create new table with CID list
    df = pd.DataFrame(columns=['cid_list', 'todays_results'])

    # get todays results
    today_results = get_todays_results(df, cid_array)
    today_results['todays_results'] = today_results['todays_results'].astype(str)
    today_results['todays_results'] = today_results['todays_results'].str.replace('[', '')
    today_results['todays_results'] = today_results['todays_results'].str.replace(']', '')
    # split into seperate columns
    split_columns = today_results['todays_results'].str.split(',', expand=True)
    df_new = pd.concat([today_results, split_columns], axis=1)
    df_new.drop('todays_results', axis=1, inplace=True)
    df_new = df_new.applymap(lambda x: str(x).replace(' ', ''))
    df_new = df_new.applymap(lambda x: str(x).replace("'", ''))
    df_new.to_csv(path + 'my_dataframe.csv', index=False)

    # Define a custom sorting function that handles None values
    def sort_row(row):
        sorted_row = [row.iloc[0]] + sorted(row.iloc[1:], key=lambda x: (x is None, x))
        return pd.Series(sorted_row, index=row.index)
        print(row.index)

    # Apply the custom sorting function to each row, excluding the first column
    df_new = df_new.apply(sort_row, axis=1)

    df_new.to_csv(path + 'my_dataframe_2.csv', index=False)

    # check which table is longer and merge
    if len(df_new.columns) > len(df_previous.columns):
        merged_df = df_new.merge(df_previous, on='cid_list', how='inner')
        delta = len(df_new.columns)
    else:
        merged_df = df_previous.merge(df_new, on='cid_list', how='inner')
        delta = len(df_previous.columns)

    # print(len(df_previous.columns))
    # print(len(df_new.columns))

    # create and concet df to merged
    for col in range(len(merged_df.columns), 150):
        merged_df[col] = ''

    merged_df = merged_df.fillna("None")

    # create df for diff

    new_diffs = []

    for row_number in range(0, len(merged_df)):
        for column_number in range(1, delta):
            if merged_df.iloc[row_number, column_number] != merged_df.iloc[row_number, column_number + delta - 1]:
                new_diffs.append(merged_df.iloc[row_number, 0] + "_" + merged_df.iloc[row_number, column_number] + "_" +
                                 merged_df.iloc[row_number, column_number + delta - 1])
                # print(new_diffs)
                # print(merged_df.iloc[row_number, 0])
                # print(merged_df.iloc[row_number, column_number], merged_df.iloc[row_number, column_number + delta - 1])

    df_diffs = pd.DataFrame(new_diffs, columns=['Diffs'])
    df_diffs.to_csv(path + 'differences.csv', index=False)
    print(df_diffs)

    # replace previous table with new one
    os.rename(path + 'my_dataframe_2.csv', path + 'my_dataframe_1.csv')

    # email
    email_list = "michal.reuveni@biocatch.com"
    message = Mail(
        from_email='no-reply@biocatch.com',
        to_emails=email_list,
        subject="subject",
        html_content='<strong>API Requests Results</strong>'

    )
    # attache excel file
    file_path = path + 'differences.csv'

    with open(file_path, 'rb') as f:
        data = f.read()
        file_data_base64 = base64.b64encode(data).decode('utf-8')
        f.close()

    attachedFile = Attachment(
        FileContent(file_data_base64),
        FileName(os.path.basename(file_path)),
        FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        Disposition('attachment')
    )
    message.attachment = attachedFile

    try:
        sg = SendGridAPIClient(api_key=sendgrid_key)
        response = sg.send(message)
        print("Email sent successfully")
    except Exception as e:
        print(str(e))


if __name__ == "__main__":
    main()
