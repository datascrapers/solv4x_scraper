import os, sys

import datascrapers.scraper as scraper
from datascrapers.db import DB

def main():
  CSV_FILENAME = 'EnergyData.csv'

  eia_scraper = scraper.EIAScraper(output_type='pandas')

  print('scraping EIA.gov and converting to Pandas DataFrame...')
  df = eia_scraper.scrape()

  #print(f'saving to {os.getcwd()}{os.sep}{CSV_FILENAME}...')
  #df.to_csv(CSV_FILENAME)
  #print('done.')

  db = DB(data=df)
  db.upload_data()

if __name__ == '__main__':
  sys.exit(main())
