'''
This process is hosted on an AWS Lambda functon that runs daily at 9am. It checks
the website for updates and sends an email if there are any new updates. An update
could be a new country added to the list, or an existing country that has been updated.
Updates will be processed by hand and added to dataset and posted at a later date.
'''

import pandas as pd

def check_for_updates():
    '''
    Orchestrating function that calls all other functions in the appropiate order.
    '''
    url = 'https://www.plonkit.net/guide#2'
    df = pd.read_html(url)

    df = combine_data_tables(df)
    df = clean_data(df)

    previous_version = pd.read_csv('previous_data.csv')

    new_countries = check_for_new_countries(df, previous_version)
    new_updates = check_for_updated_countries(df, previous_version)
    
    #write_new_csv(df, new_countries, new_updates)

    return new_countries, new_updates


def check_for_expected_columns(df:pd.DataFrame):
    expected_columns = ['Flag', 'Name', 'Country Code', 'Last updated']
    if list(df.columns) != expected_columns:
        raise ValueError('Columns do not match expected columns')


def clean_data(df:pd.DataFrame):

    check_for_expected_columns(df)

    df.drop(columns=['Flag'], inplace=True)
    df = df[df['Name'] != 'Maps'].reset_index(drop=True)

    return df


def combine_data_tables(df:pd.DataFrame):
    return pd.concat(df, ignore_index=True)


def check_for_new_countries(df:pd.DataFrame, previous_version:pd.DataFrame):
    '''
    Checks for new countries in the new version of the website
    '''
    new_countries = df[~df['Name'].isin(previous_version['Name'])]
    return list(new_countries['Name'])


def check_for_updated_countries(df:pd.DataFrame, previous_version:pd.DataFrame):
    '''
    Checks for updated countries in the new version of the website.
    Combines the two dataframes and drops duplicates to find the updated countries.
    Duplicates are checked on both the Name and Last updated columns.
    '''
    combined_df = pd.concat([df, previous_version], ignore_index=True)
    combined_df.drop_duplicates(subset=['Name', 'Last updated'], keep=False, inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    # store updates
    new_updates = {}
    
    for i in range(len(combined_df)):
        new_updates[combined_df['Name'][i]] = combined_df['Last updated'][i]
                        
    return new_updates


def write_new_csv(data, new_countries, new_updates):
    '''
    Writes a new csv if there are new countries or updates
    for the next time it checks.
    '''
    if new_countries or new_updates:
        data.to_csv('previous_data.csv', index=False)
