import time
import re

from dataclasses import dataclass, field

import requests
import pandas as pd

# NOTE: want dataclass(kw_only=True) for python 3.10+

@dataclass
class Scraper():
  DEFAULT_OUTPUT_TYPE = 'json'

  url:         str  = None
  params:      dict = None
  output_type: str  = DEFAULT_OUTPUT_TYPE

  response: requests.Response = None

  def __post_init__(self):
    if not self.url:
      raise Exception('no URL specified.')

    self._output = self._get_output_type(self.output_type)

  def _get_output_type(self, output_type):
    output_func_map = {
      'string': self.to_str,
      'json':   self.to_json,
      'pandas': self.to_pd,
    }

    output_func = output_func_map.get(output_type)

    if not output_func:
      raise Exception('output type not supported.')

    return output_func

  def scrape(self):
    try:
      self.response = requests.get(self.url, params=self.params)

      return self._output()

    except Exception as e: # of course, we'd want to actually check specific exceptions
      print(f'scraper.py: scrape(): problem sending HTTP request: {e}')

      return None

  def to_str(self) -> str:
    if self.response is None:
      raise Exception('No response')

    return self.response.text

  def to_json(self) -> dict:
    if self.response is None:
      raise Exception('No response')

    try:
      json_obj = self.response.json()

      self.response.raise_for_status()

      return json_obj
    except Exception as e:
      print(f'scraper.py: to_json(): could not convert to a JSON object: {e})')

      return None

  def to_pd(self):
    pass

@dataclass
class EIAScraper(Scraper):

  series_ids: list = field(default_factory=lambda: [
    'EBA.CAL-ALL.DF.H',
    'EBA.CAL-ALL.NG.SUN.H',
    'EBA.CAL-ALL.NG.WND.H',
  ])

  url:    str  = 'https://api.eia.gov/series/'
  params: dict = field(default_factory=lambda: {
      'api_key':   'g0m4Rggs1pnUJyMrHxlbzelyaS9GVcyeoIqqoo28',
      'series_id': '',
      'start':     time.strftime("%Y%m%d"), # start == current day
  })

  def __post_init__(self):
    super().__post_init__()

    self.params['series_id'] = self._construct_series_id()

  def _construct_series_id(self):
    return ';'.join(self.series_ids)

  # override to_json to get the exact data we need
  def to_json(self):
    json_obj = super().to_json()

    try:
      # filter the series dicts to only contain name and data keys
      filtered_json_obj = [ { k: series[k] for k in {'name', 'data'} }
                            for series in json_obj['series'] ]
      # cleanup 'name' keys
      # NOTE: every name key in EIA output has the format:
      # [data type] for [place]...
      for series in filtered_json_obj:
        series['name'] = re.sub(' for.*', '', series['name'])

      return filtered_json_obj
    except (KeyError, IndexError):
      raise Exception(json_obj['data'].get('error') or json_obj)

  def to_pd(self):
    json_obj = self.to_json()

    df = None
    for series in json_obj:
      if df is None:
        df = pd.DataFrame(series['data'],
                          columns = ['Date', series['name']])

        df.set_index('Date', drop=True, inplace=True)
      else:
        tmp_df = pd.DataFrame(series['data'],
                              columns = ['Date', series['name']])
        # outer merge because we don't want to assume which column will
        # be largest.
        df = df.merge(tmp_df, how='outer', left_on='Date', right_on='Date')

    return df.sort_values(by=['Date'], ascending=False)
