import pandas as pd


def check_for_updates():
    '''
    Get data from website
    check against previous iteration
    if new data, send email
    else, do nothing
    '''
    url = 'https://www.plonkit.net/guide#2'
    df = pd.read_html(url)

    df = combine_data_tables(df)
    df = clean_data(df)

    previous_version = pd.read_csv('previous_data.csv')

    new_countries = check_for_new_countries(df, previous_version)
    new_updates = check_for_updated_countries(df, previous_version)

    print(new_updates)


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
    return new_countries


def check_for_updated_countries(df:pd.DataFrame, previous_version:pd.DataFrame):
    '''
    Checks for updated countries in the new version of the website
    '''
    previous_version.set_index('Name', inplace=True, drop=True)
    df.set_index('Name', inplace=True, drop=True)

    new_updates = {}

    for country in range(len(previous_version)):
        if previous_version['Last updated'][country] != df['Last updated'][country]:
            new_updates[country] = df['Last updated'][country]

    return new_updates


if __name__ == '__main__':
    check_for_updates()