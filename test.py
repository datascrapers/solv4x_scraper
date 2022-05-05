import scraper

def main():
    eia_scraper = scraper.EIAScraper(output_type='pandas')

    # just downloading to CWD for now.
    eia_scraper.scrape().to_csv('EnergyData.csv')

main()
