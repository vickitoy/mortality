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
        labels = 'fips_state,fips_county,year,race_sex,age_death,icd,recode,number_of_deaths\n'
        myfile.write(labels)
        

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
                
                newline = str(int(line[:2])) + ',' + str(int(line[2:5])) + ',' +\
                        str(int(line[5:9])) + ',' +  str(int(line[9])) + ',' +\
                        str(int(line[10:12])) + ',' +  str(int(line[12:16])) + ',' +\
                        str(int(line[16:19])) + ',' +  str(int(line[19:23])) + '\n'
                myfile.write(newline)
        
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