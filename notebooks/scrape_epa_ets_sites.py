from os import mkdir
from pathlib import Path

from scrapy import Spider
from scrapy.crawler import CrawlerProcess

from dublin_building_stock.download import download

DATA_DIR = Path("data")
DOWNLOAD_DIR = DATA_DIR / "EPA-ETS"


class EpaEtsSpider(Spider):
    name = "epa-ets"
    start_urls = [
        "http://www.epa.ie/climate/emissionstradingoverview/etscheme/accesstocurrentpermits/#d.en.64017D"
    ]

    def parse(self, response):
        download_pages = [
            "http://www.epa.ie" + x.get()
            for x in response.xpath("//table//td").css("a::attr(href)")
        ]
        yield from response.follow_all(download_pages, self.parse_download_page)

    def parse_download_page(self, response):
        download_url = response.css(".download-now").xpath("@href").get()
        yield response.follow(download_url, self.save_pdf)

    def save_pdf(self, response):
        filename = response.url.split("/")[-1]
        download(response.url, DOWNLOAD_DIR / filename)


if not DOWNLOAD_DIR.exists():
    mkdir(DOWNLOAD_DIR)
process = CrawlerProcess(
    settings={
        "FEEDS": {
            "items.json": {"format": "json"},
        },
    }
)
process.crawl(EpaEtsSpider)
process.start()
