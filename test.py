import scraper

def main():
    eia_scraper = scraper.EIAScraper()

    print(eia_scraper.scrape())

main()
