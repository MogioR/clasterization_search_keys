from Modules.Tokenizer.tokenizer import Tokenizer
from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsMorphTagger, NewsNERTagger, Doc
from sklearn.feature_extraction.text import CountVectorizer

from nltk.corpus import stopwords


ALPHABET = ["а", "б", "в", "г", "д", "е", "ё", "ж", "з", "и", "й", "к", "л", "м", "н", "о", " ",
            "п", "р", "с", "т", "у", "ф", "х", "ц", "ч", "ш", "щ", "ъ", "ы", "ь", "э", "ю", "я",
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z"]
ALPHABET = list(map(lambda x: x.upper(), ALPHABET)) + ALPHABET


class NatashaTokenizer(Tokenizer):
    def __init__(self):
        self.natasha_emb = NewsEmbedding()
        self.natasha_ner_tagger = NewsNERTagger(self.natasha_emb)
        self.natasha_segmenter = Segmenter()
        self.natasha_morph_vocab = MorphVocab()
        self.natasha_morph_tagger = NewsMorphTagger(self.natasha_emb)
        self.stop_words = stopwords.words('russian')

    # Return clear string
    def clear_string(self, string):
        string = ''.join([letter if letter in ALPHABET else ' ' for letter in string])
        string = ' '.join([word for word in string.split() if word not in self.stop_words])
        return string

    # Return lemma string
    def lemma(self, string):
        string = self.clear_string(string)
        doc = Doc(string)
        doc.segment(self.natasha_segmenter)
        doc.tag_morph(self.natasha_morph_tagger)

        for token in doc.tokens:
            token.lemmatize(self.natasha_morph_vocab)

        lemma = ' '.join([_.lemma for _ in doc.tokens])
        return lemma

    # Return vectors, tokens of strings
    def vectorize(self, strings):
        if len(strings) == 0:
            return [], []

        vectorizer = CountVectorizer(token_pattern=r'[a-zA-ZА-ЯЁа-яё]{2,}')
        matrix = vectorizer.fit_transform(strings)
        return matrix.toarray(), vectorizer.get_feature_names_out()
