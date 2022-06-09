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
  'EBA.CAL-ALL.DF.HL':     'Demand Forecast',
  'EBA.CAL-ALL.NG.SUN.HL': 'Solar Generation',
  'EBA.CAL-ALL.NG.WND.HL': 'Wind Generation',
}

# log an error message then exit the program cleanly.
def err(err_msg):
  print(err_msg)
  sys.exit(1)

# return the date 12AM of T - @at days in EIA's time format or ISO format if isoFormat=True
# for EIA, we set local=True to make sure the start parameter asks for 12AM localtime.
def get_previous_day(at, isoFormat=False, local=False):
  prev_day = datetime.now() - timedelta(at)

  timezone = 'L' if local else 'Z'

  if isoFormat:
    return prev_day.strftime(f'%Y-%m-%dT00:00:00.000{timezone}')

  return prev_day.strftime(f'%Y%m%dT00{timezone}')

# returns the date of 12AM yesterday in EIA's time format.
def yesterday():
  return get_previous_day(1, local=True)

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

# format date columns to be human readable in the database
def format_date(df):
  new_utc_format   = '%Y-%m-%dT%H:%M:%S.000Z' # min, sec, and ms are always zero
  new_local_format = '%Y-%m-%dT%H:%M:%S.000%z' # local time with UTC offset

  df.insert(1, 'LocalDate', pd.to_datetime(df.Date).dt.strftime(new_local_format))
  df['Date'] = pd.to_datetime(df.Date, utc=True).dt.strftime(new_utc_format)

# convert EIA JSON output to a Pandas Dataframe
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

# convert new Dataframe back into a JSON object for the Firebase database
def pd_to_json(df, save_file=None, as_dict=False, orient='index'):
  if orient != 'table':
    # set index label to the date
    df.set_axis(df['Date'], axis='index', inplace=True)

  # orient='index' sets the keys (rows) to index labels
  # double_precision = 0 essentially casts float to int
  # orient='table' is used for the JSON file
  json_str = df.to_json(path_or_buf=save_file, orient=orient, double_precision=0)

  if save_file is not None:
    return

  if as_dict:
    return json.loads(json_str)

  return json_str

# print the dataframe to stdout
def print_df(df):
  if not isinstance(df, pd.DataFrame):
    return

  print(df)

# create a CSV file from the DataFrame
def save_csv(df):
  df.to_csv(config.CSVFilePath)

# create a JSON file from the DataFrame
# orient='table' used for easier Tableau integration
def save_json(df):
  pd_to_json(df, save_file=config.JSONFilePath, orient='table')

# prune old rows in Firebase database if rotateDatabase is set
# called in upload_to_db()
def rotate_db(db_collection):
  if config.rotateDatabase < 2:
    return

  # get the oldest day to keep, in localtime
  # [:-1] cuts off the Z at the end of the date, because we want to check the localtime of 00:00.
  # the UTC offset can be ignored because it will be the same for all rows and it won't affect our `where` query.
  oldest_day_to_keep = get_previous_day(config.rotateDatabase, isoFormat=True)[:-1]

  rows_to_prune = db_collection.where('LocalDate', '<', oldest_day_to_keep).stream()

  if rows_to_prune:
    print('db: pruning old database rows...')

  for row in rows_to_prune:
    row.reference.delete()

def upload_to_db(df):
  # Initialize the database credentials
  cred = credentials.Certificate(config.firestoreKeyPath)
  firebase_admin.initialize_app(cred)

  # Database Connection
  db = firestore.client()

  # root collection reference
  db_root_ref = db.collection('California Renewable Energy')

  df_json = pd_to_json(df, as_dict=True)

  rotate_db(db_root_ref)

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
  rotateDatabase: int = 0

  def __post_init__(self):
    self._config_dir.mkdir(parents=True, exist_ok=True)

    self._path = self._config_dir / 'config.yml'

    self.CSVFilePath      = self._config_dir / 'EnergyData.csv'
    self.JSONFilePath     = self._config_dir / 'EnergyData.json'
    self.firestoreKeyPath = self._config_dir / 'firestoreCreds.json'

  def add_config_file(self, config_file):
    for opt, val in config_file.items():
      if hasattr(self, opt):
        if val is None:
          print(f'ignoring empty value for `{opt}` in config file.')
        else:
          setattr(self, opt, val)
      else:
        print(f'ignoring invalid option `{opt}` in config file.')

# configuration initialization
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

  print('scraping EIA.gov...')
  json_output = scrape()
  print('converting JSON to a pandas DataFrame...')
  df = json_to_pd(json_output)

  if config.printData:
    print_df(df)

  if config.saveCSVFile:
    print('saving CSV file...')
    save_csv(df)

  if config.saveJSONFile:
    print('saving JSON file...')
    save_json(df)

  if config.saveToDatabase:
    print('uploading to database...')
    upload_to_db(df)

  return 0

if __name__ == '__main__':
  sys.exit(main())
