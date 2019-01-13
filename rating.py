#!/usr/bin/env python3
import os
import re
import sys
import csv
import queries
import getopt
from MySQLdb import connections
import SelectResult

COLUMNS = (
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

SYS_USER = os.getenv("USER") # get system user
CONFIG = {
    'user': 'dimasty' if SYS_USER == 'dimasty' else 'root',
    'password': 'dimasty' if SYS_USER == 'dimasty' else '',
    'host': 'localhost',
    'database': 'stat'
}

db = connections.Connection(**CONFIG)
dbc = db.cursor()
db.set_character_set('utf8')
dbc.execute('SET NAMES utf8;')
dbc.execute('SET CHARACTER SET utf8;')
dbc.execute('SET character_set_connection=utf8;')


class InputData(object):
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
        reader = csv.DictReader(self.fh, fieldnames=COLUMNS, dialect=dialect)
        if has_header:
            next(reader, None)                               # to exclude the first row (headers)
        for rec in reader:
            yield rec

    def check_rec_format(self):
        gen = self.get_records()
        for r in gen:
            record_obj = DataRecord(r)
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
                record_obj = DataRecord(r)
                record_obj.fix_it_up()
                yield record_obj.get_record()


class DataRecord(object):
    """Represents one Stat record"""

    def __init__(self, record):
        self.r = record
        self.isRecordValid = True

    def get_record(self):
        return self.r

    def fix_it_up(self):
        """
            Divide trainer_name_1 on two fields if there are two trainers;
            Add 01.01 if dd.mm (date) value is empty and concatenate it with year field;
            @return: fixed (modified) dict
        """
        del self.get_record()['id'] # delete the first column field

        trainers = [s.strip() for s in self.r['trainer_name_1'].split(",")]
        if len(trainers) == 3:
            print("[WARN] 3 trainers are encountered for {}".format(self.r['name_soname']))
            sys.exit()
        elif len(trainers) == 2:
            if trainers[0] == trainers[1]:
                print("[WARN] two identical trainer names are encountered in one record")
                sys.exit()
            if not trainers[0] or not trainers[1]:
                print("[WARN] one of two trainer names is empty for {}".format(self.r['name_soname']))
                sys.exit()
            self.r['trainer_name_1'], self.r['trainer_name_2'] = trainers
        elif len(trainers) == 1:
            trainers.append('н/д')
            self.r['trainer_name_1'], self.r['trainer_name_2'] = trainers

        # if date value is empty set it to "01.01" and add "year" value to it
        self.r['date'] = self.r['date'] if self.r['date'] else "01.01"
        self.r['date'] = ".".join([self.r['date'], self.r['year']])

    def check_col_format(self, regexps):
        """Checking record values by provided regexps from col_formats"""
        search_result = dict()
        for key in regexps:
            search_result[key] = dict()
            for regexp in regexps[key]:
                search_obj = re.search(regexp, self.r[key])
                search_result[key][regexp] = search_obj.string if search_obj else search_obj
            if not any(search_result[key].values()):
                self.isRecordValid = False
                print("""[{0}] does not match [{1}] field format. Please check records with [{2}]"""
                      .format(self.r[key], key, self.r["name_soname"]))


def usage():
    print("\nThis is the usage function\n")
    print("Usage: " + sys.argv[0] + " -f <file> or --data-file=<file>]")


def get_duplicate_name():
    try:
        dbc.execute(queries.FIND_PERSON_DUPLICATES)
    except connections.Error as e:
        print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
    return dbc


def get_regdata_by_name(participant):
    try:
        dbc.execute(queries.GetPersonalInfoByName, {"name": participant})
    except connections.Error as e:
        print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
    return dbc


try:
    opts, args = getopt.getopt(sys.argv[1:], "dhf:", [
                               "help", "schools", "data-file=", "by-name="])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
for o, a in opts:
    if o in ("-h", "--help"):
        usage()
        sys.exit()
    elif o in ("-f", "--data-file"):
        file = a
        if not os.path.isfile(file):
            print('argument is not a file')
            sys.exit()
        try:
            dbc.execute(queries.ClearRecordsTable)
            dbc.execute(queries.ClearAthletesTable)
            dbc.execute(queries.CLEAR_TRAINERS_TABLE)
            with open(file, newline='') as f:
                for dict_params in InputData(f).check_rec_format().get_data_per_row():
                    dbc.execute(queries.addRecordFromFile, dict_params)
                dbc.execute(queries.FillInAthletesTable)
                dbc.execute(queries.AddAthleteIdToStatRecord)
                dbc.execute(queries.FILL_IN_TRAINERS_TABLE)
        except connections.Error as e:
            print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
        else:
            db.commit()
    elif o in ("-d",):
        q_object = get_duplicate_name()
        SelectResult.SelectResult(q_object).print_qresult()
    elif o in ("--by-name"):
        q_object = get_regdata_by_name(a)
        result = SelectResult.SelectResult(q_object).print_qresult().report_tofile("duplicates.csv")
    elif o in ("--schools"):
        try:
            dbc.execute(queries.GET_SCHOOLS)
            SelectResult.SelectResult(dbc).print_qresult()
        except connections.Error as e:
            print("MySQL Error {0}: {1}".format(e.args[0], e.args[1]))
    else:
        assert False, "unhandled option"
db.close()
