ClearRecordsTable = '''DELETE FROM Stat_Records'''
ClearAthletesTable = '''DELETE FROM Athletes'''

FillInAthletesTable = '''
                        INSERT INTO Athletes (name_soname, date, sex)
                        SELECT DISTINCT name_soname, date, sex FROM Stat_Records'''

addRecordFromFile = '''
    INSERT INTO Stat_Records(name_soname, date, year, sex, city, school, club, competition, comp_date,
           comp_location, event_type, result, position, scores, scores_2, scores_3, scores_4, scores_5, scores_total,
           teacher_name_1, teacher_name_2)
    VALUES (%s, STR_TO_DATE(%s, "%%d.%%m.%%Y"), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

findPersoneDuplicate = ''' SELECT new.name_soname, count(new.name_soname) AS count
        FROM (SELECT DISTINCT name_soname, date, sex, city, school, club FROM Stat_Records) as new
        GROUP BY new.name_soname
        HAVING count>1'''

# subselect contains only name_soname, date and sex fields
findPersoneDuplicateLessStrict = ''' SELECT new.name_soname, count(new.name_soname) AS count
        FROM (SELECT DISTINCT name_soname, date, sex FROM Stat_Records) as new
        GROUP BY new.name_soname
        HAVING count>1'''

GetPersonalInfoByName = '''SELECT i_athlete, name_soname, date, sex, city, school, club, teacher_name_1, teacher_name_2, count(name_soname) AS duplicates
        FROM Stat_Records
        WHERE name_soname=%(name)s
        GROUP BY i_athlete, name_soname, date, sex, city, school, club, teacher_name_1, teacher_name_2'''

GetSchools = '''SELECT DISTINCT school FROM Stat_Records'''

GetCities = '''SELECT DISTINCT city FROM Stat_Records'''

GetClubs = '''SELECT DISTINCT club FROM Stat_Records'''


AddAthleteIdToStatRecord = '''
    UPDATE Stat_Records r, (SELECT i_athlete, name_soname, date, sex FROM Athletes) a
    SET r.i_athlete = a.i_athlete
    WHERE r.name_soname = a.name_soname
     AND r.date = a.date
     AND r.sex = a.sex'''