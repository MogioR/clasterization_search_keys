import requests
from bs4 import BeautifulSoup

from .query_item import QueryItem
# from Modules.Logger.logger import get_logger
#
# logger = get_logger(__name__)


class XmlRiver:
    def __init__(self, config):
        self.xml_river_user = config['XMLRiver']['xml_river_user']
        self.xml_river_key = config['XMLRiver']['xml_river_key']
        self.group_by = config['XMLRiver']['group_by']

    # Request search results from XMLRiver
    def get_search_results(self, keys: str) -> str:
        errors = ''
        report = ''
        for i in range(3):
            # logger.info('XMLRiver request: ' + keys)
            report = requests.get(self.get_request(keys), timeout=60).text
            errors = BeautifulSoup(report, 'lxml')
            if errors.yandexsearch.response.error is not None:
                pass
                # logger.warn('XMLRiver response error: ' + str(errors.yandexsearch.response.error.string) +
                #             ' try make request again.')
            else:
                break

        if errors.yandexsearch.response.error is not None:
            raise Exception('XmlRiver response error: ' + str(errors.yandexsearch.response.error.string))

        return report

    # Set locale for search
    def set_region(self, locale: str) -> None:
        pass

    # Return request to xml river api with keys
    def get_request(self, keys: str) -> str:
        pass

    # Get query data from keys
    def get_query_items(self, keys: str) -> list:
        try:
            xml_report = self.get_search_results(keys)
        except Exception as e:
            raise e

        try:
            soup = BeautifulSoup(xml_report, "html.parser")
            docs = soup.findAll('doc')

            query_items = []
            for doc in docs:
                query_items.append(QueryItem(doc))

        except Exception as e:
            query_items = []
            raise Exception('XmlRiver can\'t parse report: ' + xml_report)

        return query_items

    # Get query data from keys
    def get_query_items_with_params(self, key: str, relatives: bool = True, questions: bool = True) -> (list, list):
        try:
            xml_report = self.get_search_results(key)
        except Exception as e:
            raise e

        try:
            soup = BeautifulSoup(xml_report, "html.parser")
            docs = soup.findAll('doc')
            result = dict()

            query_items = []
            for doc in docs:
                query_items.append(QueryItem(doc))
            result['sites'] = query_items

            if relatives:
                relatives = []
                if soup.response.addresults is not None and soup.response.addresults.relatedsearches is not None:
                    relatives_raw = soup.response.addresults.relatedsearches.findAll('title')
                    for relative in relatives_raw:
                        relatives.append(relative.string)
                result['relatives'] = relatives

            if questions:
                questions = []
                if soup.response.addresults is not None and soup.response.addresults.relatedquestions is not None:
                    questions_raw = soup.response.addresults.relatedquestions.findAll('title')
                    for question in questions_raw:
                        questions.append(str(question.string).split('?')[0])

                result['questions'] = questions
                pass

        except Exception as e:
            raise Exception('XmlRiver can\'t parse report: ' + xml_report)

        return result
