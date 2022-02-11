from bs4 import BeautifulSoup


class QueryItem:
    def __init__(self, doc: BeautifulSoup):
        self.url = doc.url.text

        title = doc.title.text.replace('![CDATA[', '')
        title = title.replace(']]', '')
        title = title.replace('\xa0', '')
        self.title = title

        description = doc.passages.text.replace('![CDATA[', '')
        description = description.replace(']]', '')
        description = description.replace('\xa0', '')
        self.description = description
