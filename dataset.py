#!/usr/bin/env python3

import csv
from statrecord import StatRecord

FIELD_NAMES = (
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


class InputDataSet:
    """prepare/parse csv file with data separated by "|" """
    def __init__(self, file_obj):
        self.fh = file_obj
        self.isCheckPassed = True
        self.isCheckDone = False

    def refresh_data(self):
        """Sets the file's current position at the beginning"""
        self.fh.seek(0)

    def get_records(self):
        """
            Return (generate) records from stat file one by one in form of dictionaries
            @yield: record in form of dict is returned on each iteration
        """
        if self.fh.tell():  # if file's position is not at the beginning
            self.refresh_data()
        has_header = csv.Sniffer().has_header(self.fh.read(2048))  # check if header exists by sample
        self.fh.seek(0)                                            # setting the file's current position at 0
        dialect = csv.Sniffer().sniff(self.fh.read(2048))
        self.fh.seek(0)
        reader = csv.DictReader(self.fh, fieldnames=FIELD_NAMES, dialect=dialect)
        if has_header:
            next(reader, None)                               # to exclude the first row (headers)
        for rec in reader:
            # print(rec)
            yield rec

    def check_rec_format(self):
        gen = self.get_records()
        for r in gen:
            record_obj = StatRecord(r)
            record_obj.fix_it_up()
            record_obj.check_col_format(COL_FORMATS)
            self.isCheckDone = True
            if not record_obj.isRecordValid:
                self.isCheckPassed = False
        return self

    def get_data_per_row(self):
        if not self.isCheckPassed:
            print("Data validation was not passed")
            exit()
        elif not self.isCheckDone:
            print("Data was not validated")
            exit()
        else:
            gen = self.get_records()
            for r in gen:
                record_obj = StatRecord(r)
                record_obj.fix_it_up()
                yield record_obj.get_record()