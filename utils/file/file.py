import csv
import json
import os
import io
from typing import Tuple


def load_json(filename):
    if not os.path.exists(f'{filename}.json'): return None
    with open(f'{filename}.json', encoding='UTF-8') as f:
        return json.load(f)

def save_json(base, filename):
    with open(f'{filename}.json', "w", encoding='UTF-8') as f:
        f.write(json.dumps(base, indent=4, ensure_ascii=False))

def load_txt(filename, symbol_split = '\n'):
    if not os.path.exists(f'{filename}.txt'): return None
    with open(f'{filename}.txt', encoding='UTF-8') as f:
        return f.read().split(symbol_split)

def load_csv(filename):
    with open(f'{filename}.csv', 'w', encoding='UTF-8') as f:
        return f.read()

def save_csv(base, filename):
    with open(f'{filename}.csv', 'w', encoding='UTF-8') as f:
        f.write(base)

def save_txt(base, filename, symbol_split = '\n'):
    with open(f'{filename}.txt', 'w', encoding='UTF-8') as f:
        f.write(symbol_split.join(base))

def csv_convert_json(base_csv) -> Tuple[dict]:
    return tuple(csv.DictReader(io.StringIO(base_csv)))
