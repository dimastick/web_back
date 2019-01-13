#!/usr/bin/env python3

import csv
import sys
import re
from collections import OrderedDict

FIELDS = (
    "id", "name_soname", "date", "year", "sex", "city", "school", "club", "competition", "comp_date",
    "comp_location", "event_type", "result", "position", "scores", "scores_2", "scores_3",
    "scores_4", "scores_5", "scores_total", "trainer_name_1"
)


TRAINER_NAME_COL_FORMAT = [
    r'^[А-Я][a-я]*(\s[А-Я]{1}\.?)?$',
    r'^[А-Я][a-я]*\s[А-Я]{1}\.\s?[А-Я]{1}\.$',
    r'^[А-Я][a-я]*\s[А-Я]{1}\s[А-Я]{1}$',
    r'^(н/д|б/т)$',
]

COL_FORMATS = dict(
    name_soname=[
        r'^[А-Я][a-я]* [А-Я][a-я]*$',
        r'^[А-Я][a-я]*-[А-Я][a-я]* [А-Я][a-я]*$'
    ],
    sex=[r'(^муж$|^жен$)'],
    date=[
        r'^\d{2}.\d{2}.\d{4}$',
        r'^$'
    ],
    trainer_name_1=TRAINER_NAME_COL_FORMAT,
    trainer_name_2=TRAINER_NAME_COL_FORMAT
)


def gen_records_from_file(file_path='../mstatistics.csv'):
    """
        Return (generate) records from stat file one by one in form of dictionaries
        @param file_path: path to source file
        @yield: record in form of dict is returned in each iteration
    """
    with open(file_path, newline='') as f:
        has_header = csv.Sniffer().has_header(f.read(2048))     # check if header exists by sample
        f.seek(0)                                               # setting the file's current position at 0
        dialect = csv.Sniffer().sniff(f.read(2048))
        f.seek(0)
        reader = csv.DictReader(f, fieldnames=FIELDS, dialect=dialect)
        if has_header:
            next(reader, None)                                  # to exclude the first row (headers)
        for rec in reader:
            yield rec


def fixup_stat_record(dict_record):
    """
        Divide trainer_name_1 on two fields if there are two trainers;
        Add 01.01 if dd.mm (date) value is empty and concatenate it with year field;
        @param dict_record: record in form of dict
        @return: fixed (modified) dict
    """
    trainers = [s.strip() for s in dict_record['trainer_name_1'].split(",")]
    if len(trainers) == 3:
        print("[WARN] 3 trainers are encountered for {}".format(dict_record['name_soname']))
        sys.exit()
    elif len(trainers) == 2:
        if trainers[0] == trainers[1]:
            print("[WARN] two identical trainer names are encountered in one record")
            sys.exit()
        if not trainers[0] or not trainers[1]:
            print("[WARN] one of two trainer names is empty for {}".format(dict_record['name_soname']))
            sys.exit()
        dict_record['trainer_name_1'], dict_record['trainer_name_2'] = trainers
    elif len(trainers) == 1:
        trainers.append('н/д')
        dict_record['trainer_name_1'], dict_record['trainer_name_2'] = trainers

    # if date value is empty set it to "01.01" and add "year" value to it
    dict_record['date'] = dict_record['date'] if dict_record['date'] else "01.01"
    dict_record['date'] = ".".join([dict_record['date'], dict_record['year']])
    print(dict_record)
    return dict_record


def check_col_format(regexps, dict_record):
    """Checking record values by provided regexps from col_formats"""
    search_result = dict()
    for key in regexps:
        search_result[key] = dict()
        for regexp in regexps[key]:
            search_obj = re.search(regexp, dict_record[key])
            search_result[key][regexp] = search_obj.string if search_obj else search_obj
        if not any(search_result[key].values()):
            print("""[{0}] does not match [{1}] field format. Please check records with [{2}]"""
                  .format(dict_record[key], key, dict_record["name_soname"]))


if __name__ == "__main__":
    gen_dicts = gen_records_from_file()

    check_col_format(COL_FORMATS, fixup_stat_record(next(gen_dicts)))