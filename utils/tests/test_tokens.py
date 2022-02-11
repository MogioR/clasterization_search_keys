from utils.tokens import Token, Tokens
from utils.tokens.tokens import (BLACK_WORDS, WORDS_IGNORE_NORMALIZE,
                                 WORDS_REPLACE)

def test_Token():
    token:Token = Token('Тестовое')
    assert token.word == 'Тестовое'
    assert token.text == 'тестовый'
    assert token.tag == 'ADJF'

def test_Tokens():
    tokens:Tokens = Tokens('Идёт иди 123 Лает лай')
    tokens2:Tokens = Tokens('Идёт иди')

    assert tokens.text_org == 'Идёт иди 123 Лает лай'
    assert len(tokens.base_tokens) == 3
    assert len(tokens.get_text()) == 14
    assert len(Tokens.check(tokens, tokens2)) == 1

def test_black_words():
    global BLACK_WORDS
    BLACK_WORDS.append('тест')
    tokens:Tokens = Tokens('тест тестом погоняет')
    assert len(tokens.base_tokens) == 1
    assert 'погонять' == list(tokens.base_tokens)[0].text
    BLACK_WORDS.remove('тест')

def test_replace_words():
    global WORDS_REPLACE
    WORDS_REPLACE.update({'тестовый':'тест', 'тестом':'тест'})
    tokens:Tokens = Tokens('тестовый тест тестом погоняет тест')
    assert len(tokens.base_tokens) == 2
    text = tokens.get_text()
    assert not 'тестовый' in text
    assert not 'тестом' in text
    WORDS_REPLACE.pop('тестовый')
    WORDS_REPLACE.pop('тестом')

def test_ignore_normalize_words():
    global WORDS_IGNORE_NORMALIZE
    WORDS_IGNORE_NORMALIZE.append('тестят')
    WORDS_IGNORE_NORMALIZE.append('тесто')
    tokens:Tokens = Tokens('Тесты тестят тесто тестом')
    assert len(tokens.base_tokens) == 3
    text = tokens.get_text()
    assert 'тестят' in text
    assert 'тесто' in text
    assert 'тест' in text
    WORDS_IGNORE_NORMALIZE.remove('тестят')
    WORDS_IGNORE_NORMALIZE.remove('тесто')

if __name__ == '__main__':
    test_Token()
    test_Tokens()
    test_black_words()
    test_replace_words()
    test_ignore_normalize_words()
