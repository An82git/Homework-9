# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy.exporters import JsonItemExporter

from tools.download_json import quotes_path, authors_path


class ItemPipeline:

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get("quote"):

            data = {
                "tags": adapter.get("tags"),
                "author": adapter.get("author"),
                "quote": adapter.get("quote"), 
            }
            
            adapter["data"] = data
            adapter["file"] = quotes_path
        elif adapter.get("fullname"):

            description: str = adapter.get("description")
            description = description.replace("\n", "").strip()
        
            data = {
                "fullname": adapter.get("fullname"),
                "born_date": adapter.get("born_date"),
                "born_location": adapter.get("born_location"),
                "description": description
            }
            
            adapter["data"] = data
            adapter["file"] = authors_path
        else:
            raise DropItem(f"{item} does not have a fullname or quote field")

        return item

class JsonWriterPipeline:
    def open_spider(self, spider):
        self.exporters = {}

    def close_spider(self, spider):
        for exporter, json_file in self.exporters.values():
            exporter.finish_exporting()
            json_file.close()

    def _exporter_for_item(self, item):
        adapter = ItemAdapter(item)

        file_path = adapter["file"]
        if file_path not in self.exporters:
            json_file = open(file_path, "wb")
            exporter = JsonItemExporter(json_file, indent = 2)
            exporter.start_exporting()
            self.exporters[file_path] = (exporter, json_file)
        return self.exporters[file_path][0]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        exporter = self._exporter_for_item(item)
        exporter.export_item(adapter["data"])
        return item
