import os, sys
from pathlib import Path # used for config and CSV file creation
from datetime import datetime, timedelta # used for 'from' param for EIA API call

from dataclasses import dataclass, field, fields # used for config object
import yaml # used for config file
import json # used for JSON file

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

# returns the date of 12AM yesterday in EIA's time format.
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

def format_date(df):
  new_format = '%Y-%m-%dT%H:%M:%S.000Z' # min, sec, and ms are always zero

  df['Date'] = pd.to_datetime(df.Date)
  df['Date'] = df['Date'].dt.strftime(new_format)

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

  format_date(df)

  return df

def pd_to_json(df, save_file=None, as_dict=False):
  # set index label to the date
  df.set_axis(df['Date'], axis='index', inplace=True)

  # orient='index' sets the keys (rows) to index labels
  # double_precision = 0 essentially casts float to int
  json_str = df.to_json(path_or_buf=save_file, orient='index', double_precision=0)

  if save_file is not None:
    return

  if as_dict:
    return json.loads(json_str)

  return json_str

def print_df(df):
  if not isinstance(df, pd.DataFrame):
    return

  # prints out dataframe to stdout
  print(df)

def save_csv(df):
  # create a csv from the DataFrame
  df.to_csv(config.CSVFilePath)

def save_json(df):
  pd_to_json(df, save_file=config.JSONFilePath)

def upload_to_db(df):
  # Initialize the database credentials
  cred = credentials.Certificate(config.firestoreKeyPath)
  firebase_admin.initialize_app(cred)

  # Database Connection
  db = firestore.client()

  # root collection reference
  db_root_ref = db.collection('California Renewable Energy')

  df_json = pd_to_json(df, as_dict=True)

  # update database in one atomic operation
  batch = db.batch()
  for date, cols in df_json.items():
    row_doc = db_root_ref.document(date)
    batch.set(row_doc, cols, merge=True)
  batch.commit()

# create a Config dataclass and initialize it with default config options
# exceptions raised are caught in init_config()
@dataclass
class Config:
  _config_dir: Path = Path.home() / '.datascrapers'
  _path: Path = None

  EIA_APIKey: str = 'L08gD6TFlLdYhl1sJKagbCVA5AJmcjCVOlWbUEdz'

  printData: bool = False

  saveCSVFile: bool = True
  CSVFilePath: Path = None

  saveJSONFile: bool = True
  JSONFilePath: Path = None

  firestoreKeyPath: Path = None
  saveToDatabase: bool = True

  def __post_init__(self):
    self._config_dir.mkdir(parents=True, exist_ok=True)

    self._path = self._config_dir / 'config.yml'

    self.CSVFilePath      = self._config_dir / 'EnergyData.csv'
    self.JSONFilePath     = self._config_dir / 'EnergyData.json'
    self.firestoreKeyPath = self._config_dir / 'firestoreCreds.json'

  def add_config_file(self, config_file):
    for opt, val in config_file.items():
      if hasattr(config, opt):
        if val is None:
          print(f'ignoring empty value for `{opt}` in config file.')
        else:
          setattr(config, opt, val)
      else:
        print(f'ignoring invalid option `{opt}` in config file.')

def init_config():
  # `config` is global so that any function can access the config settings
  global config

  try:
    config = Config()
  except Exception as e:
    err(f'problem creating config object: {e}')
    
  # open the YAML config file and reassign options it specifies in Config
  try:
    with open(config._path) as config_file:
      config_file = yaml.safe_load(config_file) # loaded as a dict

      config.add_config_file(config_file)
  except (FileNotFoundError, yaml.YAMLError) as e:
    print(f'problem loading config file: {e}')
    print('using default config settings.')

def main():
  init_config()

  json_output = scrape()
  df = json_to_pd(json_output)

  if config.printData:
    print_df(df)

  if config.saveCSVFile:
    save_csv(df)

  if config.saveJSONFile:
    save_json(df)

  if config.saveToDatabase:
    upload_to_db(df)

  return 0

if __name__ == '__main__':
  sys.exit(main())
