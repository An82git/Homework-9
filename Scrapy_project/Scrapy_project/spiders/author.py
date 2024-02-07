from typing import Any, Optional
import scrapy
import time

from Scrapy_project.items import AuthorItem
from tools.connect import connect_rabbit


class AuthorSpider(scrapy.Spider):
    name = "author"
    allowed_domains = ["quotes.toscrape.com"]

    # def __init__(self, name: str | None = None, **kwargs: Any):
    #     super(AuthorSpider, self).__init__(name, **kwargs)
    #     url = kwargs.get("url")

    def start_requests(self):
        # yield scrapy.Request(self.url)
        for url in self.receiving_queue("author"):
            yield scrapy.Request(url=url)

    # def callback(self, ch, method, properties, body):
    #     if body:
    #         data = body.decode()
            
    #         if data == "End":
    #             ch.stop_consuming()
    #         else:
    #             self.url_list.append(data)

    def receiving_queue(self, queue):
        with connect_rabbit() as con:
            ch = con.channel()
            ch.queue_declare(queue=queue)
            
            while True:
                method, properties, body = ch.basic_get(queue=queue, auto_ack=True)

                if method:
                    data = body.decode()
                
                    if data == "End":
                        break
                    else:
                        yield data
                else:
                    break

                
                # time.sleep(0.5)

        
            # ch.basic_consume(queue=queue, on_message_callback=self.callback, auto_ack=True)
            # ch.start_consuming() 
                                
    def quote_item(sefl, author):
        author_item = AuthorItem()

        author_item["fullname"] = author.xpath("h3[@class='author-title']/text()").get()
        author_item["born_date"] = author.xpath("p/span[@class='author-born-date']/text()").get()
        author_item["born_location"] = author.xpath("p/span[@class='author-born-location']/text()").get()
        author_item["description"] = author.xpath("div[@class='author-description']/text()").get()

        return author_item
    
    def parse(self, response):
        author = response.xpath("/html//div[@class='author-details']")

        yield self.quote_item(author)
        