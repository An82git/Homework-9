from mongoengine import Document
from mongoengine.fields import ReferenceField, StringField, ListField


class Authors(Document):
    fullname = StringField()
    born_date = StringField()
    born_location = StringField()
    description = StringField()


class Quotes(Document):
    tags = ListField()
    author = ReferenceField("Authors")
    quote = StringField()