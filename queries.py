ClearRecordsTable = '''DELETE FROM Stat_Records'''
ClearAthletesTable = '''DELETE FROM Athletes'''
CLEAR_TRAINERS_TABLE = '''DELETE FROM Trainers'''

FillInAthletesTable = '''
                        INSERT INTO Athletes (name_soname, date, sex)
                        SELECT DISTINCT name_soname, date, sex FROM Stat_Records'''

addRecordFromFile = '''
    INSERT INTO Stat_Records(name_soname, date, year, sex, city, school, club, competition, comp_date,
           comp_location, event_type, result, position, scores, scores_2, scores_3, scores_4, scores_5, scores_total,
           trainer_name_1, trainer_name_2)
    VALUES (
        %(name_soname)s,
        STR_TO_DATE(%(date)s, "%%d.%%m.%%Y"),
        %(year)s,
        %(sex)s,
        %(city)s,
        %(school)s,
        %(club)s,
        %(competition)s,
        %(comp_date)s,
        %(comp_location)s,
        %(event_type)s,
        %(result)s,
        %(position)s,
        %(scores)s,
        %(scores_2)s,
        %(scores_3)s,
        %(scores_4)s,
        %(scores_5)s,
        %(scores_total)s,
        %(trainer_name_1)s,
        %(trainer_name_2)s
        )'''

FILL_IN_TRAINERS_TABLE = '''
INSERT INTO Trainers (name, scores) 
SELECT trainer_name_1 AS trainer, SUM(scores) AS scores
FROM 
    (
        (SELECT trainer_name_1, SUM(scores_total)/2 AS scores
         FROM Stat_Records 
         WHERE trainer_name_2 <> 'н/д'
         GROUP BY trainer_name_1
         ORDER BY trainer_name_1)
    UNION ALL
        (SELECT trainer_name_2, SUM(scores_total)/2 AS scores
         FROM Stat_Records
         WHERE trainer_name_2 <> 'н/д'
         GROUP BY trainer_name_1
         ORDER BY trainer_name_1)
    UNION ALL
        (SELECT trainer_name_1, SUM(scores_total) AS scores
         FROM Stat_Records
         WHERE trainer_name_2 = 'н/д'
         GROUP BY trainer_name_1
         ORDER BY trainer_name_1)
    ) t
GROUP BY trainer
ORDER BY scores DESC;
'''

FIND_PERSON_DUPLICATES = ''' SELECT new.name_soname, count(new.name_soname) AS count
        FROM (SELECT DISTINCT name_soname, date, sex, city, school, club FROM Stat_Records) as new
        GROUP BY new.name_soname
        HAVING count>1'''

# subselect contains only name_soname, date and sex fields
FIND_PERSON_DUPLICATES_LESS_STRICT = ''' SELECT new.name_soname, count(new.name_soname) AS count
        FROM (SELECT DISTINCT name_soname, date, sex FROM Stat_Records) as new
        GROUP BY new.name_soname
        HAVING count>1'''

GetPersonalInfoByName = '''SELECT i_athlete, name_soname, date, sex, city, school, club, trainer_name_1, trainer_name_2, count(name_soname) AS duplicates
        FROM Stat_Records
        WHERE name_soname=%(name)s
        GROUP BY i_athlete, name_soname, date, sex, city, school, club, trainer_name_1, trainer_name_2'''

GET_SCHOOLS = '''SELECT DISTINCT school FROM Stat_Records'''

GetCities = '''SELECT DISTINCT city FROM Stat_Records'''

GetClubs = '''SELECT DISTINCT club FROM Stat_Records'''


AddAthleteIdToStatRecord = '''
    UPDATE Stat_Records r, (SELECT i_athlete, name_soname, date, sex FROM Athletes) a
    SET r.i_athlete = a.i_athlete
    WHERE r.name_soname = a.name_soname
     AND r.date = a.date
     AND r.sex = a.sex'''