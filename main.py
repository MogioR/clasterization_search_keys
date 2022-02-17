import os
import sys
import json

from api_objects import BaseContainer
from Modules.сlusterer_search_keys import ClustererSearchKeysService
from Modules.GoogleApi import GoogleSheetsApi
from Modules.GeoFiches.geo_fiches import GeoFiches
from Modules.Tokenizer.natasha_tokenizer import NatashaTokenizer

CONFIG = {
    "XMLRiver": {
        "xml_river_user": "1660",
        "xml_river_key": "9d9ea875799adf551c8329d0a6dcf50ed168f9b8",
        "group_by": 10,
        "Google": {
            "default_country_id": 2112,
            "default_loc_id": 1001493,
            "default_language_id": "RU",
            "default_device": "desktop",
            "default_use_language": False
        },
        "Yandex": {
            "default_loc_id": 4,
            "default_language_id": "ru",
            "default_device": "desktop",
            "default_use_language": False
        }
    }
}

if __name__ == '__main__':
    # Fix api_objects path for Pickle
    sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '\\Modules')

    API_TOKEN = 'Environment/google_token.json'
    SEARCH_ENGINE = 'GOOGLE'
    DOMAIN = 'brest.kuku.by'
    CLUSTERING_LEVEL = 1
    GOOGLE_DOCUMENT_OUT = '18CSD7sNaJWQ4DDOv6omd0J2jSYuT7xjlKCyAxSdz-QQ'
    GOOGLE_DOCUMENT_IN = '1LRE5onYv7TB6XIQhlUiAuOVrul-c8jvP3l8OKQx_CkA'
    LIST_NAME = '/remont-tehniki2'
    MAX_COUNT_OF_LINKS = 1000       # 0 - no limit

    stop_words = ['купить', 'отзывы', 'бесплатно', 'спб', 'форум']
    inclusion_words = ['стоимость', 'цена', 'прайс', 'заказать', 'заказ', 'стоит', 'цены', 'на дом', 'на час', 'услуги']

    base_container = BaseContainer()
    base_container.load()
    level_containers = base_container.get_containers(domain=DOMAIN, hidden=False, level_mode=True,
                                                     level=CLUSTERING_LEVEL)


    def get_childes(key: str, main_container: BaseContainer) -> list:
        childes = []
        if key in main_container.base_parents:
            for child in base_container.base_parents[key]:
                childes.append(child)
                childes = childes + get_childes(child.id, main_container)

        return childes


    base_clusters = []
    for c in level_containers:
        base_cluster = [c]
        childes_list = get_childes(c.id, base_container)
        for child in childes_list:
            base_cluster.append(child)
        base_clusters.append(base_cluster)

    service = ClustererSearchKeysService(API_TOKEN, SEARCH_ENGINE, CONFIG)
    service.set_containers(base_clusters[0])
    service.clear()

    googleService = GoogleSheetsApi('Environment/google_token.json')

    raw_data = googleService.get_data_from_sheets(GOOGLE_DOCUMENT_IN,
                                                  LIST_NAME,
                                                  'A2', 'G'+str(googleService.get_list_size(GOOGLE_DOCUMENT_IN,
                                                                                            LIST_NAME)[1]), 'COLUMNS')

    if MAX_COUNT_OF_LINKS != 0 and len(raw_data[0]) > MAX_COUNT_OF_LINKS:
        raw_data[0] = raw_data[0][:MAX_COUNT_OF_LINKS]

    # service.urls = raw_data[0]
    for url in raw_data[0]:
        url = url.replace('https://', '')
        domain = url[0:url.find('/')]
        root = url[url.find('/'):len(url)]
        if url.find('fake') == -1:
            service.urls.append(['https://'+domain, root])

    geo_names = []
    clear_geos = set()
    main_names = []
    clear_mains = set()
    old_anchors = set()
    geo_fiches = GeoFiches(NatashaTokenizer())
    for i in range(len(raw_data[0])):
        if len(raw_data) > 6 and len(raw_data[6]) > i and len(raw_data[6][i]) > 0:
            buf = str(raw_data[6][i]).split(',')
            for b in buf:
                old_anchors.add(geo_fiches.lemma_of_query(b))

        if len(raw_data) > 5 and len(raw_data[5]) > i and len(raw_data[5][i]) > 0:
            main_names.append(raw_data[1][i])
            clear_mains.add(geo_fiches.clear_extract_geo(raw_data[5][i]))
        elif len(raw_data) > 4:
            geo_names.append(raw_data[1][i])
            clear_geos.add(geo_fiches.clear_extract_geo(raw_data[4][i]))

    service.geo_container_names = geo_names
    service.main_container_names = main_names

    service.clear_geo = clear_geos
    service.clear_cities = clear_mains

    service.set_inclusion_words(inclusion_words)
    service.set_stop_words(stop_words)
    service.old_anchors = list(old_anchors)

    del base_container
    del level_containers
    del base_clusters

    print('Get stats')
    service.get_stats()
    print('Get clusters')
    clusters_anchors = service.make_clusters(0.8)
    clusters_six = service.make_clusters(0.6)
    print('Make reports')

    service.make_report_to_sheets(clusters_anchors, GOOGLE_DOCUMENT_IN, LIST_NAME+'_clusters_anchors', True)
    service.make_report_to_sheets(clusters_six, GOOGLE_DOCUMENT_IN, LIST_NAME+'_clusters_six', False)

    with open(LIST_NAME+'_del.json', "w", encoding='utf-8') as write_file:
        json.dump(service.deleted, write_file, ensure_ascii=False, indent=4)
