import scrapy

from Scrapy_project.items import QuoteItem
from tools.connect import connect_rabbit


class QuoteSpider(scrapy.Spider):
    name = "quote"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["http://quotes.toscrape.com"]

    def sending_queue(self, queue, data):
        with connect_rabbit() as connection:
            channel = connection.channel()
            channel.queue_declare(queue=queue)
            for url in data:
                channel.basic_publish(
                    exchange='',
                    routing_key=queue,
                    body=url.encode()
                    )

    def quote_item(sefl, quote):
        quote_item = QuoteItem()

        quote_item["quote"] = quote.xpath("span[@class='text']/text()").get()
        quote_item["author"] = quote.xpath("span/small[@class='author']/text()").get()
        quote_item["tags"] = quote.xpath("div[@class='tags']/a/text()").getall()

        return quote_item

    def parse(self, response):
        quotes = response.xpath("/html//div[@class='quote']")
        authors_url = []

        for quote in quotes:
            author_url = quote.xpath("span/a/@href").get()
            authors_url.append(f"{self.start_urls[0]}{author_url}")
            yield self.quote_item(quote)
        
        self.sending_queue("author", authors_url)
        next_page = response.xpath("/html//li[@class='next']/a/@href").get()
        url = f"{self.start_urls[0]}{next_page}"
        yield {"next_page": url}
        
        if next_page is not None:
            yield scrapy.Request(url=url)
        else:
            self.sending_queue("author", ["End"])
