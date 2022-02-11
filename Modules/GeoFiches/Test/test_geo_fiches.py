# clear_extract_geo
# geoService.clear_extract_geo('в Минске') -> Минске
# geoService.clear_extract_geo('в городе Минск') -> Минск
# geoService.clear_extract_geo('в садоводческом товариществе Хрюничево') -> Хрюничево

# lemma_of_query
# geoService.lemma_of_query('массаж каменная горка') -> массаж каменный горка
# geoService.lemma_of_query('массаж на каменной горке') -> массаж каменный горка
# geoService.lemma_of_query('массаж в Уфе') -> массаж уфа
# geoService.lemma_of_query('массаж в городе Уфа') -> массаж уфа

# check_geo_include
# geoService.check_geo_include('массаж на каменной горке', 'каменная горка') -> true
# geoService.check_geo_include('массаж в Уфе', 'Уфа') -> true
