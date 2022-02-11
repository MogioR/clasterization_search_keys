import json
import re
import copy

from tqdm import tqdm
from multiprocessing import Pool
from multiprocessing_logging import install_mp_handler

from Modules.Xml_river import YandexXmlRiver, GoogleXmlRiver
from Modules.GoogleApi import GoogleSearchConsoleApi, GoogleSheetsApi
from Modules.сlusterer_service import ClusteringService
from Modules.GeoFiches.geo_fiches import GeoFiches
from Modules.Tokenizer.natasha_tokenizer import NatashaTokenizer

# from Modules.Logger.logger import get_logger

# logger = get_logger(__name__)
# install_mp_handler()

NUM_THREADS = 8
COUNT_OF_EXTRA_REQUESTS = 3


class ClustererSearchKeysService:
    def __init__(self, google_token: str, search_engine: str, xml_river_config: dict):
        """
            :param google_token: token for google table
            :param search_engine: type of search engine (YANDEX/GOOGLE)
            :param xml_river_config: xml_river_config dict
        """
        self.google_token = google_token

        if search_engine == 'GOOGLE':
            self.river = GoogleXmlRiver(xml_river_config)
        else:
            self.river = YandexXmlRiver(xml_river_config)

        self.urls = list()
        self.geo_container_names = set()
        self.main_container_names = set()
        self.clear_geo = set()
        self.clear_cities = set()

        self.inclusion_words = list()
        self.stop_words = list()
        self.old_anchors = list()

        self.deleted = list()
        self.made_queries = list()
        self.results_queries = list()
        self.results = list()
        self.geo_queries = list()

    # Set containers to clustering
    def set_containers(self, containers: list):
        self.clear()
        geo_fiches = GeoFiches(NatashaTokenizer())
        for container in containers:
            self.urls.append([container.domain, container.path])
            if container.extract_city is not None:
                self.clear_cities.add(geo_fiches.clear_extract_geo(container.extract_city))

            if container.extract_name is not None:
                self.main_container_names.add(container.extract_name)

            if container.extract_geo is not None:
                self.geo_container_names.add(container.name)
                self.clear_geo.add(geo_fiches.clear_extract_geo(container.extract_geo))

            for anchor_name in container.anchor_name:
                self.old_anchors.append(anchor_name)

        self.old_anchors = self.del_duplicates(self.old_anchors)

    def set_inclusion_words(self, inclusion_words: list):
        geo_fiches = GeoFiches(NatashaTokenizer())
        for inclusion_word in inclusion_words:
            self.inclusion_words.append(geo_fiches.lemma_of_query(inclusion_word))

    def set_stop_words(self, stop_words: list):
        geo_fiches = GeoFiches(NatashaTokenizer())
        for stop_word in stop_words:
            self.stop_words.append(geo_fiches.lemma_of_query(stop_word))

    # Return list with gsc keys
    def get_gsc_queries(self, url) -> list:
        try:
            gsc_service = GoogleSearchConsoleApi(self.google_token)
        except Exception as error:
            raise Exception('Stopped in authorisation error: ' + str(error))

        result = []
        error = None
        for i in range(3):
            try:
                result = gsc_service.get_keys_by_url(url[0], url[1])
                error = None
                break
            except Exception as e:
                # logger.info("Can't download gsc queries for {0}. Error: {1}. Try again.".format(url[0]+url[1], error))
                error = e

        if error is not None:
            result = []
            # logger.warn("Can't download gsc queries for {0}. Error: {1}. Ignore url".format(url[0]+url[1], error))
            print('Error in ' + url[0]+url[1] + ' error: ' + str(error))

        return result

    # Filter queries del (delete duplicates and not valid queries)
    def filter_gsc_queries(self, queries: list, min_clicks: int = 1, min_impressions: int = 50) -> list:
        queries_text = [query[0] for query in queries]
        clusters = ClusteringService().cluster_list(queries_text, 1)

        result = list()
        added_clusters = list()
        to_delete = list()
        for i, cluster in enumerate(clusters):
            if cluster not in added_clusters:
                if queries[i][1] >= min_clicks or queries[i][2] >= min_impressions:
                    result.append(queries[i])
                    added_clusters.append(cluster)
                else:
                    to_delete.append(queries[i][0])

        with open('backup_gsc.json', "w", encoding='utf-8') as write_file:
            json.dump(queries_text, write_file, ensure_ascii=False, indent=4)

        self.deleted.append([to_delete, 'Не проходят по условиям (отсев gsc)'])

        return result

    # Sort queries by geo and main
    def sort_queries(self, queries: list, stage: int) -> (list, list):
        geo_fiches = GeoFiches(NatashaTokenizer())
        geo_queries = []
        main_queries = []
        to_delete = []
        for query in queries:
            lemma = geo_fiches.lemma_of_query(query[0])
            if not self.regularity_check(lemma, self.stop_words):
                if self.regularity_check(lemma, list(self.clear_geo)):
                    geo_queries.append([query[0], query[1], query[2], lemma])
                # elif self.regularity_check(lemma, self.inclusion_words + list(self.clear_cities)):
                elif self.regularity_check(lemma, list(self.clear_cities)) or \
                        self.regularity_check(lemma, self.inclusion_words):
                    main_queries.append([query[0], query[1], query[2], lemma])
                else:
                    to_delete.append(query[0])
            else:
                to_delete.append(query[0])

        buf = copy.deepcopy(to_delete)
        self.deleted.append([buf, 'Не гео и не мэйн стадия ' + str(stage)])

        to_delete.clear()
        geo_queries = self.del_duplicates(geo_queries, index=3, to_delete=to_delete)
        buf = copy.deepcopy(to_delete)
        self.deleted.append([buf, 'Гео дубликаты стадия ' + str(stage)])

        to_delete.clear()
        main_queries = self.del_duplicates(main_queries, index=3, to_delete=to_delete)
        buf = copy.deepcopy(to_delete)
        self.deleted.append([buf, 'Мэйн дубликаты стадия ' + str(stage)])

        return geo_queries, main_queries

    # Make query for queries by XMLRiver
    def get_search_results(self, queries: list, stage: int):
        results = list()
        relatives_keys_list = list()
        relatives_questions_list = list()

        pool = Pool(NUM_THREADS)
        try:
            only_keys = [_[0] for _ in queries]
            print('STAGE:', stage)
            for key, links, rel_keys, rel_questions in tqdm(pool.imap(self.query, only_keys), total=len(only_keys)):
                results.append([key, links])
                relatives_keys_list += rel_keys.split('|')
                relatives_questions_list += rel_questions.split('|')

        except Exception as e:
            raise Exception('Error: ' + str(e))

        self.made_queries += queries

        to_delete = list()
        relatives_keys_list = self.del_duplicates(relatives_keys_list, to_delete=to_delete)
        relatives_questions_list = self.del_duplicates(relatives_questions_list, to_delete=to_delete)
        self.deleted.append([to_delete, 'Дубликаты связанных ' + str(stage)])

        return results, relatives_keys_list, relatives_questions_list

    # Return key and urls from search engine request with key
    def query(self, key: str) -> (str, str, str, str):
        error = None
        for i in range(3):
            try:
                result = self.river.get_query_items_with_params(key)
                item_urls = []
                for item in result['sites']:
                    item_urls.append(item.url)

                relatives_keys = result['relatives'] if 'relatives' in result.keys() else []
                relatives_questions = result['questions'] if 'questions' in result.keys() else []

                return key, ' '.join(item_urls), '|'.join(relatives_keys), '|'.join(relatives_questions)

            except Exception as e:
                error = e

        if error is not None:
            raise Exception('Stopped in ' + key + ' error: ' + str(error))

    # Make new queries by relatives
    def relatives_to_queries(self, relatives_keys: list, relatives_questions: list, stage: int) -> list:
        geo_fiches = GeoFiches(NatashaTokenizer())

        new_queries = [[_, -2-10 * stage, -2-10 * stage, geo_fiches.lemma_of_query(_)] for _ in relatives_keys]
        new_queries += [[_, -3-10 * stage, -3-10 * stage, geo_fiches.lemma_of_query(_)] for _ in relatives_questions]

        # Del made queries
        unique_new_queries = list()
        to_delete = []
        only_lemma = self.old_anchors
        only_lemma += [query[3] for query in self.made_queries + new_queries]
        clusters = ClusteringService().cluster_list(only_lemma, 1.0)

        for i, cluster in enumerate(clusters[len(self.made_queries) + len(self.old_anchors):]):
            if i + len(self.made_queries) + len(self.old_anchors) == cluster:
                unique_new_queries.append(new_queries[i])
            else:
                to_delete.append(new_queries[i][0])

        self.deleted.append([to_delete, 'дубликаты стадия ' + str(stage)])

        return unique_new_queries

    # Get data from search engine
    def get_stats(self):
        # Get first queries
        gsc_queries = list()
        for url in tqdm(self.urls):
            gsc_queries += self.get_gsc_queries(url)

        queries = [[_, -1, -1] for _ in self.main_container_names]
        queries += [[_, -1, -1] for _ in self.geo_container_names]
        queries += self.filter_gsc_queries(gsc_queries)
        del gsc_queries

        stage = 1
        for step in range(COUNT_OF_EXTRA_REQUESTS):
            # Sort queries
            geo_queries, main_queries = self.sort_queries(queries, stage)
            self.geo_queries += geo_queries
            stage += 1

            # Get results
            _, relatives_keys, relatives_questions = self.get_search_results(geo_queries, stage)
            results, relatives_keys_buf, relatives_questions_buf = self.get_search_results(main_queries, stage)
            self.results_queries += main_queries
            self.results += results
            relatives_keys += relatives_keys_buf
            relatives_questions += relatives_questions_buf
            stage += 1

            # Search new queries
            queries = self.relatives_to_queries(relatives_keys, relatives_questions, stage)
            stage += 1

            if len(queries) == 0:
                break

    # Return clusters of self.normalised_keys with similarity [0..1]
    def make_clusters(self, similarity: float) -> list:
        only_urls = [key[1] for key in self.results]
        clusters = ClusteringService.cluster_list(only_urls, similarity)
        return clusters

    def make_report_to_json(self, clusters: list, file_out: str):
        printed = []
        out_obj = {'clusters': []}
        for cluster in clusters:
            if cluster not in printed:
                cluster_obj = {'keys': [], 'urls': []}
                printed.append(cluster)
                for i in range(len(clusters)):
                    if clusters[i] == cluster:
                        cluster_obj['keys'].append(self.results[i][0])
                        cluster_obj['urls'].append(self.results[i][1].split(' '))

                out_obj['clusters'].append(cluster_obj)

        with open(file_out, "w", encoding='utf-8') as write_file:
            json.dump(out_obj, write_file, ensure_ascii=False)

    def make_report_to_sheets(self, clusters: list, document_id: str, list_name: str, anchors: bool = False):
        output_data = []
        printed = []
        clusters_pos = []
        to_delete = list()

        shift = 2
        geo_fiches = GeoFiches(NatashaTokenizer())
        all_geo = self.clear_geo.union(self.clear_cities)
        for cluster in clusters:
            if cluster not in printed:
                printed.append(cluster)

                cluster_buf = []
                container_flag = False
                for i in range(len(clusters)):
                    if clusters[i] == cluster:
                        cluster_buf.append([''] + self.results_queries[i])

                        # Удаляем гео
                        cluster_buf[-1][1] = geo_fiches.delete_geo(cluster_buf[-1][1], all_geo)

                        if self.results_queries[i][1] == -1:
                            cluster_buf[-1][0] = cluster_buf[-1][1]
                            container_flag = True
                        elif self.results_queries[i][1] == -32:
                            cluster_buf[-1][0] = 'ПЗ_rs_1'
                        elif self.results_queries[i][1] == -33:
                            cluster_buf[-1][0] = 'ПЗ_rq_1'
                        elif self.results_queries[i][1] == -62:
                            cluster_buf[-1][0] = 'ПЗ_rs_2'
                        elif self.results_queries[i][1] == -63:
                            cluster_buf[-1][0] = 'ПЗ_rq_2'

                cluster_buf_clear = self.del_duplicates(cluster_buf, index=1, to_delete=to_delete)

                if len(cluster_buf_clear) > 1:
                    if anchors is True and container_flag:
                        start_pos = len(output_data) + shift
                        output_data += cluster_buf_clear
                        end_pos = len(output_data) + shift - 1
                        output_data.append(['', '', '', ''])
                        clusters_pos.append([start_pos, end_pos])
                    elif anchors is False and container_flag is False:
                        start_pos = len(output_data) + shift
                        output_data += cluster_buf_clear
                        end_pos = len(output_data) + shift - 1
                        output_data.append(['', '', '', ''])
                        clusters_pos.append([start_pos, end_pos])

        print_list = []
        if not anchors:
            buf = [geo_fiches.lemma_of_query(geo_fiches.delete_geo(_[0], self.clear_geo.union(self.clear_cities)))
                   for _ in self.geo_queries]

            clusters = ClusteringService.cluster_list(buf, 1.0)
            for i, cluster in enumerate(clusters):
                if i == cluster or self.geo_queries[i][1] == -1:
                    print_list.append(self.geo_queries[i])

        sheets_api = GoogleSheetsApi(self.google_token)
        header = ['id', 'query', 'clicks', 'impressions']
        # Create list
        try:
            if anchors:
                sheets_api.create_sheet(document_id, list_name, 1 + len(output_data), len(header))
            else:
                sheets_api.create_sheet(document_id, list_name, 1 + len(output_data) + len(self.geo_queries),
                                        len(header))
            # logger.info("Created new sheet with name: " + name_of_sheet)
        except Exception as e:
            # logger.info("Sheet " + name_of_sheet + " already created, recreate")
            sheets_api.delete_sheet(document_id, list_name)
            if anchors:
                sheets_api.create_sheet(document_id, list_name, 1 + len(output_data), len(header))
            else:
                sheets_api.create_sheet(document_id, list_name, 1 + len(output_data) + len(print_list),
                                        len(header))

        sheets_api.put_row_to_sheets(document_id, list_name, 1, 'A', header)
        sheets_api.put_data_to_sheets(document_id, list_name, 'A2', 'E' + str(1 + len(output_data)), 'ROWS',
                                      output_data)

        if not anchors:
            sheets_api.put_data_to_sheets(document_id, list_name, 'A' + str(2 + len(output_data)),
                                          'E' + str(1 + len(output_data) + len(print_list)),
                                          'ROWS', print_list)
        for pos in clusters_pos:
            sheets_api.create_group(document_id, list_name, pos[0], pos[1], 'ROWS')

        self.deleted.append([to_delete, 'Дубликаты в кластере'])

    def clear(self):
        self.urls.clear()
        self.geo_container_names.clear()
        self.main_container_names.clear()
        self.clear_geo.clear()
        self.deleted.clear()

    # Return "direct" including words in list dictionary in string data.
    @staticmethod
    def regularity_check(data: str, dictionary: list):
        including = False
        data_str = str(data).lower()

        for word in dictionary:
            including = including or (
                    re.search(r'^' + word.lower() + '[^A-Za-zА-ЯЁа-яё]|[^A-Za-zА-ЯЁа-яё]' + word.lower() +
                              '[^A-Za-zА-ЯЁа-яё]|[^A-Za-zА-ЯЁа-яё]' + word.lower() + '$'
                              , data_str) is not None)
            if including is True:
                break

        return including

    # Del duplicates in list by index
    @staticmethod
    def del_duplicates(data: list, index: int = None, to_delete: list = None) -> list:
        if index is not None:
            text = [_[index] for _ in data]
        else:
            text = data

        if len(text) == 0:
            return []

        clusters = ClusteringService().cluster_list(text, 1.0)

        result = list()
        for i, cluster in enumerate(clusters):
            if i == cluster:
                result.append(data[i])
            if to_delete is not None:
                if index is not None:
                    to_delete.append(data[i][0])
                else:
                    to_delete.append(data[i])
        return result
