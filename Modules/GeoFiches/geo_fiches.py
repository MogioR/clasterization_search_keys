import re

PREFIXES = ('д.', 'п.', 'аг.', 'ст.', 'гп.', 'м.', 'р-н', 'мкр.', 'деревне', 'посёлке', 'агрогородке',
            'садоводческом товариществе', 'городском посёлке', 'районе', 'микрорайоне', 'у метро', 'м.', 'у м.',
            'городе', 'г.')

PREFIXES_LEMMA = ('город', 'метро', 'район', 'посёлок', 'микрорайон', 'деревня', 'агрогородок', ' в ', ' во ')
PREFIXES_LEMMA_WITHOUT_SPACES = ('город', 'метро', 'район', 'посёлок', 'микрорайон', 'деревня', 'агрогородок', 'в',
                                 'во', 'на', 'г.', 'г')


class GeoFiches:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    # Return lemma extract_geo without prefix
    def clear_extract_geo(self, extract_geo: str) -> str:
        result = extract_geo.lower()

        if result.find('в ') == 0:
            result = result[2:]
        if result.find('во ') == 0:
            result = result[3:]

        for prefix in PREFIXES:
            if result.find(prefix) == 0:
                result = result[len(prefix):]
                break

        return self.lemma_of_query(result)

    # Return query without clear_geos
    def delete_geo(self, query: str, clear_geos: set) -> str:
        words = query.split(' ')
        new_query = []
        for word in words:
            if self.tokenizer.lemma(word) not in clear_geos:
                new_query.append(word)
            elif new_query[-1] in PREFIXES_LEMMA_WITHOUT_SPACES:
                new_query = new_query[0:len(new_query) - 1]

        return ' '.join(new_query)

    # Return lemma of query without of prefixes_lemma and prepositions
    def lemma_of_query(self, query: str) -> str:
        result = self.tokenizer.lemma(query)
        for prefix in PREFIXES_LEMMA:
            result = result.replace(prefix, ' ')
        result = re.sub(r'\s+', ' ', result)
        return result.strip()

    # Return true if one of clear_geos in query
    def check_geos_include(self, query: str, clear_geos: set) -> bool:
        for clear_geo in clear_geos:
            lemma_clear_geo = self.lemma_of_query(clear_geo)
            lemma_of_query = self.lemma_of_query(query)
            if lemma_of_query.find(lemma_clear_geo) != -1:
                return True
        return False
