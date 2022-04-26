import time

import requests

class Scraper():
  DEFAULT_OUTPUT_TYPE = 'json'

  def __init__(self, url=None, params=None,
               output_type=DEFAULT_OUTPUT_TYPE):
    if not url:
      raise Exception('no URL specified.')

    self.url    = url
    self.params = params
    self.output = self._set_output_type(output_type)

    self.response = None
      
  def _set_output_type(self, output_type):
    if output_type == 'string':
      return self.to_str
    if output_type == 'json':
      return self.to_json
    else:
      raise Exception('output type not supported.')

  def scrape(self):
    try:
      self.response = requests.get(self.url, params=self.params)
      return self.output()

    except Exception as e: # of course, we'd want to actually check specific exceptions
      print(f'scraper.py: scrape(): problem sending HTTP request: {str(e)}')

      return None

  def to_str(self):
    if self.response is None:
      raise Exception('No response')

    return self.response

  def to_json(self):
    if self.response is None:
      raise Exception('No response')

    try:
      res = self.response.json()

      self.response.raise_for_status()
      
      return res
    except Exception as e:
      print(f'scraper.py: to_json(): could not convert to a JSON object: {str(e)})')

      return None

class EIAScraper(Scraper):
  def __init__(self):
    Scraper.__init__(self,
      url='https://api.eia.gov/series/',
      params={
        'api_key': 'g0m4Rggs1pnUJyMrHxlbzelyaS9GVcyeoIqqoo28', # bijan's EIA API key
        'series_id': 'EBA.CAL-ALL.DF.H',
        'start': time.strftime("%Y%m%d"), # start == current day
      },
    )

  # override to_json to get the exact data we need
  def to_json(self):
    json_obj = super().to_json()

    return json_obj['series'][0]['data'] # find out why series is a list
