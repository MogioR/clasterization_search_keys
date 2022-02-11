from __future__ import annotations
import re
from typing import Dict, List, Set

import pymorphy2

from utils.file import load_txt

BLACK_WORDS = [
    "и",
    "в",
    "во",
    "не",
    "что",
    "он",
    "на",
    "я",
    "с",
    "со",
    "как",
    "а",
    "то",
    "все",
    "она",
    "так",
    "его",
    "но",
    "да",
    "ты",
    "к",
    "у",
    "же",
    "вы",
    "за",
    "бы",
    "по",
    "только",
    "ее",
    "мне",
    "было",
    "вот",
    "от",
    "меня",
    "еще",
    "нет",
    "о",
    "из",
    "ему",
    "теперь",
    "когда",
    "даже",
    "ну",
    "вдруг",
    "ли",
    "если",
    "уже",
    "или",
    "ни",
    "быть",
    "был",
    "него",
    "до",
    "вас",
    "нибудь",
    "опять",
    "уж",
    "вам",
    "ведь",
    "там",
    "потом",
    "себя",
    "ничего",
    "ей",
    "может",
    "они",
    "тут",
    "где",
    "есть",
    "надо",
    "ней",
    "для",
    "мы",
    "тебя",
    "их",
    "чем",
    "была",
    "сам",
    "чтоб",
    "без",
    "будто",
    "чего",
    "раз",
    "тоже",
    "себе",
    "под",
    "будет",
    "ж",
    "тогда",
    "кто",
    "этот",
    "того",
    "потому",
    "этого",
    "какой",
    "совсем",
    "ним",
    "здесь",
    "этом",
    "один",
    "почти",
    "мой",
    "тем",
    "чтобы",
    "нее",
    "сейчас",
    "были",
    "куда",
    "зачем",
    "всех",
    "никогда",
    "можно",
    "при",
    "наконец",
    "два",
    "об",
    "другой",
    "хоть",
    "после",
    "над",
    "больше",
    "тот",
    "через",
    "эти",
    "нас",
    "про",
    "всего",
    "них",
    "какая",
    "много",
    "разве",
    "три",
    "эту",
    "моя",
    "впрочем",
    "хорошо",
    "свою",
    "этой",
    "перед",
    "иногда",
    "лучше",
    "чуть",
    "том",
    "нельзя",
    "такой",
    "им",
    "более",
    "всегда",
    "конечно",
    "всю",
    "между",
    "цена",
    "стоимость",
    "работа",
    "работы",
    "мастер",
    "сборщик",
    "услуга",
    "услуги",
    "заказ",
    "массажист",
    "животное",
    "мастерская",
    "врач",
    "вечер",
    "проведение",
    "год",
    "праздник",
    "онлайн",
    "дистанционно",
    "профессиональный",
    "профессионал",
    "эффект"
]
WORDS_REPLACE = {
    'сделать':'делать',
    'смонтировать':'монтаж',
    "сборка":"установить",
    'собрать':'установить',
    "установка":"установить",
    'поставить':'установить',
    'подключить':'установить',
    'подключение':'установить',
    'устанавливал':'установить',
    'демонтаж':'демонтировать',
    'замена':'заменить',
    'меняли':'заменить',
    'ремонт':'починить',
    'отремонтировать':'починить',
    "починка":"починить",
    'коррекция':'корректировать',
    'cнятие':'снять',
    "оформление":"оформить",
    "декор":"декорировать",
    "покраска":"покрасить",
    "восстановление":"восстановить",
    "завивка":"завить",
    "удаление":"удалить",
    "напылить":"напыление",
    "исправление":"исправить",
    "обить":"перетяжка",
    "обивка":"перетяжка",
    "реставрация":"реставрировать",
    "диагностика":"диагностировать",
    "перепрошивка":"перепрошить",
    "прошивка":"прошить",
    "наращивание":"нарастить",
    "нарощенные":"нарастить",
    "выравнивание":"выровнять",
    'шкафчик':'шкаф',
    'столик':'стол',
    "изготовление":"заказать",
    'изготовить':'заказать',
    'заказывать':'заказать',
    'заказ':'заказать',
    'поменять':'замена',
    'дистанционное':'дистанционно',
    'прочистить':'прочистка',
    'обложить':'облицовка',
    'облицевать':'облицовка',
    'шлифовка':'отшлифовать',
    'перестановка':'переставить',
    'компьютерная':'компьютер',
    'свадебная':'свадьба',
    'фотографы':'фотосъёмка',
    'фотограф':'фотосъёмка',
    'фотография':'фотосъёмка',
    'парикмахеры':'стрижка',
    'фотосъемка':'фотосъёмка',
    'фотосессия':'фотосъёмка',
    'фото':'фотосъёмка',
    'съёмка':'фотосъёмка',
    'съемка':'фотосъёмка',
    'штукатурные':'штукатурить',
    'штукатурка':'штукатурить',
    'оштукатуривание':'штукатурить',
    'навеска':'перевесить',
    'повесить':'перевесить',
    'лечение':'лечить',
    'вылечить':'лечить',
    'ресничка':'ресница',
    'машинка':'машина',
    'гипсокартон':'гипсокартонный',
    'ванны':'ванная'
}

# список слов, которые не нормализуются и остаются, как есть.
# работа != работы
WORDS_IGNORE_NORMALIZE = [
    'работа',
    'работы',
    'работу',
    'работ',
    'вода',
    'воды',
]

SELECT_TAG_TYPES = ['NOUN', 'ADJF', 'VERB', 'INFN']


check_ignore_types = lambda token: token.tag in SELECT_TAG_TYPES # игнорируем выбранные типы
check_types = lambda token: not token.tag in SELECT_TAG_TYPES # оставляем только выбранные типы

CHECK_TYPES = check_types


def set_check_types(mode):
    '''
    Меняет поведение проверки на допустимые TAG при преобразовании в нормальную форму слов.
    mode true - допустимы слова с tag указанные в SELECT_TAG_TYPES 
    mode false - слова с тегом из SELECT_TAG_TYPES игнорируются
    '''
    global CHECK_TYPES
    CHECK_TYPES = check_types if mode else check_ignore_types

morph = pymorphy2.MorphAnalyzer()

class Token:
    text = None
    def __init__(self, word:str) -> None:
        self.word:str = word.strip()
        if self.word.isdigit(): return 
        if self.word in WORDS_IGNORE_NORMALIZE:
            self.text = self.word
            self.tag = 'NOUN'
        else:
            data:pymorphy2.analyzer.Parse = morph.parse(self.word)[0]
            text:str = data.normal_form
            self.text:str = WORDS_REPLACE.get(text, text)
            self.tag:str = str(data.tag.POS) if data.tag.POS else 'NOUN'
    
    def __repr__(self) -> str:
        return self.text

    def __hash__(self):
        return hash(self.text)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, Token) and self.text == o.text# or self.text in o.text or o.text in self.text)

class BaseTokens:
    base:Dict[str, Token] = dict()

    @classmethod
    def get(cls, word):
        if len(word) < 2: return None
        token = Token(word)
        if not token.text or token.text in BLACK_WORDS or CHECK_TYPES(token): return None
        base_token = cls.base.get(token.text)
        if base_token: return base_token
        cls.base.update({token.text:token})
        return token
# https://redsale.by/api/comments?token=6PmWUehjZMugwn8mNxdrVqyG5F3wUmm&sectionId=7659&page=0&size=100

class Tokens:
    def __init__(self, text:str) -> None:
        self.text_org = text
        text = re.sub('([0-9]) (класс)', lambda m:f'{m.group(1)}{m.group(2)}', text)
        self.base_tokens:Set[Token] = Tokens.process_tokens(re.sub('[\W ]', ' ', text.replace('под ключ', '')))

    # возвращает набор токенов в виде одной строки
    def get_text(self):return ' '.join(list(map(lambda x: x.text, self.base_tokens)))
    
    def __repr__(self) -> str:
        return self.text_org

    @classmethod
    def process_tokens(cls, text:str) -> Set[Token]:
        tokens = set()
        for word in text.split():
            token = BaseTokens.get(word)
            if token: tokens.add(token)
        return tokens

    @classmethod
    def check(cls, tokens1:Tokens, tokens2:Tokens) -> Set[Token]:
        return tokens1.base_tokens.intersection(tokens2.base_tokens)

if __name__ == '__main__':
    q1 = BaseTokens.get('собрать')
    q2 = BaseTokens.get('собрать')
    print(q1, id(q1))
    print(q2, id(q2))

