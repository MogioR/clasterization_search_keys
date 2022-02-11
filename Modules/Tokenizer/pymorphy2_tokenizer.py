import pymorphy2

from Modules.Tokenizer.tokenizer import Tokenizer
from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords


ALPHABET = ["а", "б", "в", "г", "д", "е", "ё", "ж", "з", "и", "й", "к", "л", "м", "н", "о", " ",
            "п", "р", "с", "т", "у", "ф", "х", "ц", "ч", "ш", "щ", "ъ", "ы", "ь", "э", "ю", "я",
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z"]
ALPHABET = list(map(lambda x: x.upper(), ALPHABET)) + ALPHABET
MORPH = pymorphy2.MorphAnalyzer()
STOP_WORDS = stopwords.words('russian')


class Pymorphy2Tokenizer(Tokenizer):
    # Return clear string
    @staticmethod
    def clear_string(string):
        string = ''.join([letter if letter in ALPHABET else ' ' for letter in string])
        string = ' '.join([word for word in string.split() if word not in STOP_WORDS])
        return string

    # Return lemma string
    def lemma(self, string):
        string = Pymorphy2Tokenizer.clear_string(string)
        tokens = string.split(' ')

        lemmed = []
        for token in tokens:
            lemmed.append(MORPH.parse(token)[0].normal_form)

        lemma = ' '.join(lemmed)
        return lemma

    # Return vectors, tokens of strings
    def vectorize(self, strings):
        if len(strings) == 0:
            return [], []

        vectorizer = CountVectorizer(token_pattern=r'[a-zA-ZА-ЯЁа-яё]{2,}')
        matrix = vectorizer.fit_transform(strings)
        return matrix.toarray(), vectorizer.get_feature_names_out()
