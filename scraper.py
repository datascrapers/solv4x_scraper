import os, sys
from pathlib import Path # used for config and CSV file creation
from datetime import datetime, timedelta # used for 'from' param for EIA API call

from dataclasses import dataclass, field, fields # used for config object
import yaml # used for config file

import requests
import pandas as pd

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# key: Series Id. This identifies the specific data we are targeting. You can find this in the EIA API documents.
# val: Column names that will be used for this series ID.
series_name_map = {
  'EBA.CAL-ALL.DF.H':     'Demand Forecast',
  'EBA.CAL-ALL.NG.SUN.H': 'Solar Generation',
  'EBA.CAL-ALL.NG.WND.H': 'Wind Generation',
}

# log an error message then exit the program cleanly.
def err(err_msg):
  print(err_msg)
  sys.exit(1)

def yesterday():
  return (datetime.now() - timedelta(1)).strftime('%Y%m%dT00Z')

# returns EIA API output as json object
def scrape():
  URL = 'https://api.eia.gov/series/'
  PARAMS = {
    'api_key': config.EIA_APIKey,
    'series_id': ';'.join(series_name_map),
    'start': yesterday(), # start == 12AM yesterday
  }

  try:
    r = requests.get(URL, PARAMS)

    r.raise_for_status()
  except Exception as e:
    err(f'GET request failed: {e}')

  try:
    json_data = r.json()
  except Exception as e:
    err(f'could not convert to JSON: {e}')

  if json_data.get('data') and 'error' in json_data['data']:
    err(f'API call error: {json_data["data"]["error"]}')

  return json_data

def json_to_pd(json_data):
  df = None
  # loop through series list returned by EIAs API and add them to a pandas dataframe
  for series in json_data.get('series'):
    series_name = series_name_map.get(series['series_id'])
    if series_name is None:
      err(f'No `{series["series_id"]}` key in series_name_map. Please add one manually.')

    if df is None:
      df = pd.DataFrame(series.get('data'),
                        columns = ['Date', series_name])
      df.set_index('Date', drop=True, inplace=True)
    else:
      df_to_merge = pd.DataFrame(series['data'],
                                 columns = ['Date', series_name])

      df = df.merge(df_to_merge, how='outer', left_on='Date', right_on='Date')

  assert(df is not None)

  return df

def save_csv(df):
  # create a csv from the DataFrame
  df.to_csv(config.CSVFilePath)

  # prints out dataframe
  print(df)


def upload_to_db(df):
  # Initialize the database credentials
  cred = credentials.Certificate(config.firestoreKeyPath)
  firebase_admin.initialize_app(cred)

  # Database Connection
  db = firestore.client()

  # Add Data From the Dataframe to the Database
  for _, row in df.iterrows():
    cols = {'Date': str(row['Date'])}
    for col, val in row.items():
      if col != 'Date':
        cols[col] = int(val) # XXX does EIA ever return floats that have a fractional?

    db.collection('California Renewable Energy') \
      .document(row['Date'])                     \
      .add(cols)

# create a Config dataclass and initialize it with default config options
# exceptions raised are caught in init_config()
@dataclass
class Config:
  _config_dir: Path = Path.home() / '.datascrapers'
  path: Path = None

  EIA_APIKey: str = 'L08gD6TFlLdYhl1sJKagbCVA5AJmcjCVOlWbUEdz'

  saveCSVFile: bool = True
  CSVFilePath: Path = None

  firestoreKeyPath: Path = None
  saveToDatabase: bool = True

  def __post_init__(self):
    self._config_dir.mkdir(parents=True, exist_ok=True)

    self.path = self._config_dir / 'config.yml'

    self.CSVFilePath      = self._config_dir / 'EnergyData.csv'
    self.firestoreKeyPath = self._config_dir / 'firestoreCreds.json'


def init_config():
  # `config` is global so that any function can access the config settings
  global config

  try:
    config = Config()
  except Exception as e:
    err(f'problem creating config object: {e}')
    
  # open the YAML config file and reassign options it specifies in Config
  with open(config.path) as config_file:
    try:
      config_file = yaml.safe_load(config_file) # loaded as a dict

      for opt, val in config_file.items():
        if hasattr(config, opt):
          setattr(config, opt, val)
        else:
          print(f'ignoring invalid option `{opt} in config file.')
    except yaml.YAMLError as e:
      print(f'problem loading config file: {e}')
      print('using default config settings.')

def main():
  init_config()

  json_output = scrape()
  df = json_to_pd(json_output)

  if config.saveCSVFile:
    save_csv(df)

  if config.saveToDatabase:
    upload_to_db(df)

  return 0

if __name__ == '__main__':
  sys.exit(main())
