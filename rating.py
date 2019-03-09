#!/usr/bin/env python3
import os
import sys
import queries
import getopt
from MySQLdb import connections
from dataset import InputDataSet
import SelectResult


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
''.up

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
                for dict_params in InputDataSet(f).check_rec_format().get_data_per_row():
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
