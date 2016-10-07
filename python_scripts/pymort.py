import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv
import MySQLdb as mdb

passfile = '/Users/vickitoy/sideprojects/mortality/password.txt'

def password_ret(passfile=passfile):
    
    passdict = {}
    with open(passfile, 'r') as f:
        for line in f:
            idb, iuser, ipass = line.split(',')
            passdict[idb] = (iuser, ipass)
            
    return passdict

def makemort_csv(file, newfile):

    with open(newfile, 'w') as myfile:
        labels = 'id,fips_state,fips_county,year,race_sex,age_death,icd,recode,number_of_deaths\n'
        myfile.write(labels)
        
    counter = 1
    with open(newfile, 'a') as myfile:
    
        with open(file, 'r') as f:
            for line in f:
                #print '************'
                #print line
                #print 'FIPS State', line[:2]
                #print 'FIPS county', line[2:5]
                #print 'Year', line[5:9]
                #print 'Race/sex', line[9]
                #print 'Age death', line[10:12]
                #print 'ICD', line[12:16]
                #print 'Recode', line[16:19]
                #print '# of deaths', line[19:23]
                
                newline = str(counter) + ',' + str(int(line[:2])) + ',' +\
                        str(int(line[2:5])) + ',' +\
                        str(int(line[5:9])) + ',' +  str(int(line[9])) + ',' +\
                        str(int(line[10:12])) + ',' +  str(int(line[12:16])) + ',' +\
                        str(int(line[16:19])) + ',' +  str(int(line[19:23])) + '\n'
                myfile.write(newline)
                counter +=1
        
def makemorttable(tablename, csvfile):
    
    passdict = password_ret(passfile=passfile)
    
    # connect(host, database username, password, database)
    db = 'testdb'
    
    con = mdb.connect('localhost', passdict[db][0], passdict[db][1], db)
    
    # Don't need to do error handling or closing when using the "with"
    with con:
        
        # Creates Writers table within testdb
        cur = con.cursor()   
        sqltable = """CREATE TABLE %s (
                        ID int primary key NOT NULL AUTO_INCREMENT,
                        fips_state INT,
                        fips_county INT,
                        year INT,
                        race_sex INT,
                        age_death INT,
                        icd INT,
                        recode INT,
                        num_deaths INT) 
                    """  % (tablename)
        cur.execute(sqltable)
        sqlload = """LOAD DATA LOCAL INFILE '{}'
                        INTO TABLE %s
                        FIELDS TERMINATED BY ','
                        OPTIONALLY ENCLOSED BY '"'
                        LINES TERMINATED BY '\n'
                        IGNORE 1 LINES;;"""  % (tablename)               
        cur.execute(sqlload.format(csvfile))
        
        
def makestate_csv(file, newfile):

    with open(newfile, 'w') as myfile:
        labels = 'fips_state,state_abbv,state_name,gnisid\n'
        myfile.write(labels)
        

    with open(newfile, 'a') as myfile:
    
        with open(file, 'r') as f:
            for i,line in enumerate(f):
                if i == 0: continue
                newline = line.split('|')
                newline = (',').join(newline)
                myfile.write(newline)
                
                
def makestatetable(tablename, csvfile):
    
    passdict = password_ret(passfile=passfile)
    
    # connect(host, database username, password, database)
    db = 'testdb'
    
    con = mdb.connect('localhost', passdict[db][0], passdict[db][1], db)
    
    # Don't need to do error handling or closing when using the "with"
    with con:
        
        # Creates Writers table within testdb
        cur = con.cursor()   
        sqltable = """CREATE TABLE %s (
                        fips_state INT,
                        state_abbv CHAR(2),
                        state_name VARCHAR(40),
                        gnisid INT) 
                    """  % (tablename)
        cur.execute(sqltable)
        sqlload = """LOAD DATA LOCAL INFILE '{}'
                        INTO TABLE %s
                        FIELDS TERMINATED BY ','
                        OPTIONALLY ENCLOSED BY '"'
                        LINES TERMINATED BY '\n'
                        IGNORE 1 LINES;;"""  % (tablename)               
        cur.execute(sqlload.format(csvfilepath+csvfile))
        
def makepop_csv(file, newfile):

    with open(newfile, 'w') as myfile:
        labels = 'id,fips_state,fips_county,year,race_sex,num_births,pop1_4,pop5_9,\
            pop10_14,pop15_19,pop20_24,pop25_34,pop35_44,pop45_54,pop55_64,pop65_74,\
            pop75_84,pop85,county_name,rec_type\n'
        myfile.write(labels)
        
    counter = 1
    with open(newfile, 'a') as myfile:
    
        with open(file, 'r') as f:
            for line in f:
                #print '************'
                #print line
                #print 'FIPS State', line[:2]
                #print 'FIPS county', line[2:5]
                #print 'Year', line[5:9]
                #print 'Race/sex', line[9]
                #print 'Number of live births', line[10:18]
                #print 'Pop 1-4', line[18:26]
                #print 'Pop 5-9', line[26:34]
                #print 'Pop 10-14', line[34:42]
                #print 'Pop 15-19', line[42:50]
                #print 'Pop 20-24', line[50:58]
                #print 'Pop 25-34', line[58:66]
                #print 'Pop 35-44', line[66:74]
                #print 'Pop 45-54', line[74:82]
                #print 'Pop 55-64', line[82:90]
                #print 'Pop 65-74', line[90:98]
                #print 'Pop 75-84', line[98:106]
                #print 'Pop 85+', line[106:114]
                #print 'County name', line[114:139]
                #print 'Record type', line[139]
                
                newline = str(counter) + ',' + str(int(line[:2])) + ',' +\
                        str(int(line[2:5])) + ',' +\
                        str(int(line[5:9])) + ',' +  str(int(line[9])) + ',' +\
                        str(int(line[10:18])) + ',' +  str(int(line[18:26])) + ',' +\
                        str(int(line[26:34])) + ',' +  str(int(line[34:42])) + ',' +\
                        str(int(line[42:50])) + ',' +  str(int(line[50:58])) + ',' +\
                        str(int(line[58:66])) + ',' +  str(int(line[66:74])) + ',' +\
                        str(int(line[74:82])) + ',' +  str(int(line[82:90])) + ',' +\
                        str(int(line[90:98])) + ',' +  str(int(line[98:106])) + ',' +\
                        str(int(line[106:114])) + ',' +\
                        str(line[114:139]) + ',' +\
                        str(int(line[139])) + '\n'
                myfile.write(newline)
                counter +=1
                
def makepoptable(tablename, csvfile):
    
    passdict = password_ret(passfile=passfile)
    
    # connect(host, database username, password, database)
    db = 'testdb'
    
    con = mdb.connect('localhost', passdict[db][0], passdict[db][1], db)
    
    # Don't need to do error handling or closing when using the "with"
    with con:
        
        # Creates Writers table within testdb
        cur = con.cursor()   
        sqltable = """CREATE TABLE %s (
                        ID int primary key NOT NULL AUTO_INCREMENT,
                        fips_state INT,
                        fips_county INT,
                        year INT,
                        race_sex INT,
                        num_births INT,
                        pop1_4 INT,
                        pop5_9 INT,
                        pop10_14 INT,
                        pop15_19 INT,
                        pop20_24 INT,
                        pop25_34 INT,
                        pop35_44 INT,
                        pop45_54 INT,
                        pop55_64 INT,
                        pop65_74 INT,
                        pop75_84 INT,
                        pop85 INT,
                        county_name VARCHAR(25),
                        record_type INT) 
                    """  % (tablename)
        cur.execute(sqltable)
        sqlload = """LOAD DATA LOCAL INFILE '{}'
                        INTO TABLE %s
                        FIELDS TERMINATED BY ','
                        OPTIONALLY ENCLOSED BY '"'
                        LINES TERMINATED BY '\n'
                        IGNORE 1 LINES;;"""  % (tablename)               
        cur.execute(sqlload.format(csvfile))