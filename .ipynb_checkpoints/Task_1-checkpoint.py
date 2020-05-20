#!/usr/bin/env python

from pathlib import Path
from operator import itemgetter, add
from functools import partial, reduce
import re
from collections import defaultdict, Counter
from itertools import chain

DATA_PATH = Path('./data')

file_paths =  {
    'biblio': DATA_PATH/'data-102277-2020-04-02.csv',
    'cinema': DATA_PATH/'data-102278-2020-04-02.csv',
    'culture': DATA_PATH/'data-102279-2020-04-02.csv',
    'park':  DATA_PATH/'data-4214-2020-04-02.csv'
}

def load_data(file_path, encoding):
    with open(file_path, encoding=encoding) as f:
        data = f.readlines()     
    return data

def split_data(elem, sep=';'):
    return elem.split(sep)

def is_correct_record(elem):
    return elem[0] != '"ID"'

def extract_fields(elem, indices):
    return itemgetter(*indices)(elem)

extract_addr_counts = partial(extract_fields, indices=[4, 5])
extract_park = partial(extract_fields, indices=[5])

def has_not_pattern(s, pat):
    return not re.search(pat, s)

building_pat = re.compile(r'(дом\s+|корпус\s+|строение\s+|владение\s+)')
is_not_building = partial(has_not_pattern, pat=building_pat)

def clean_data(elem):
    addr = elem[0]
    addr = split_data(addr, sep=',')
    addr = list(filter(is_not_building, addr))
    addr = addr[-1]
    addr = addr.strip().lower()
    
    count = elem[1]
    count = count.replace('"', '')
    count = int(count)
    
    return (addr, count)

def clean_park_data(elem):
    data = elem.replace('"', '')
    data = data[data.find('«'):]
    data = data.lower()
    
    return (data, 1)

def calculate_count(acc, nxt):
    acc[nxt[0]] += nxt[1]
    return acc

def process_data(file_path, encoding):
    data = load_data(file_path, encoding=encoding)
    data = map(split_data, data)
    data = filter(is_correct_record, data)
    data = map(extract_addr_counts, data)
    data = map(clean_data, data)

    return data

def process_park_data(file_path, encoding):
    data = load_data(file_path, encoding=encoding)
    data = map(split_data, data)
    data = filter(is_correct_record, data)
    data = map(extract_park, data)
    data = map(clean_park_data, data)
    
    return data

if __name__ == '__main__':
    encoding = 'windows-1251'
    data_types = ['biblio', 'cinema', 'culture']
    park_data_type = 'park'

    processed_data = map(lambda x: process_data(file_paths[x], encoding=encoding), data_types)
    processed_data = chain.from_iterable(processed_data)
    processed_park_data = process_park_data(file_paths[park_data_type], encoding=encoding)
    processed_full_data = chain(processed_data, processed_park_data)

    results = reduce(calculate_count, processed_full_data, defaultdict(int))
    results = Counter(results)
    print(results.most_common(5))