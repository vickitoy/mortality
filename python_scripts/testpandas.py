import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
import matplotlib.pyplot as plt
import time

passfile = '/Users/vickitoy/sideprojects/mortality/password.txt'

def password_ret(passfile=passfile):
    
    passdict = {}
    with open(passfile, 'r') as f:
        for line in f:
            idb, iuser, ipass = line.split(',')
            passdict[idb] = (iuser, ipass)
            
    return passdict


def totaldeaths(bygender=False):

    passdict = password_ret(passfile=passfile)
    
    # connect(host, database username, password, database)
    db = 'testdb'
    
    engine = sqlalchemy.create_engine('mysql://'+passdict[db][0]+':'+passdict[db][1]+'@localhost/'+db, echo=False)    
    conn=engine.connect()
    #start = time.time()
    
    #a=pd.read_sql('SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort6878 GROUP BY year UNION SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort7988 GROUP BY year;', conn)    
    #a.plot('year', 'total_deaths')
    #plt.savefig()
    #end = time.time()
    #print(end - start)

    start = time.time()
    a,b,c,d = testquery()
    
    mort = pd.read_sql(a.statement, conn)
    #pop = pd.read_sql(b.statement, conn)

    end = time.time()
    print(end - start)  
    print mort  
    
    if bygender:
        sexm = pd.read_sql(c.statement,conn)
        sexf = pd.read_sql(d.statement,conn)
        
        plt.bar(sm['year'], sm['total_deaths']/1e6, label='men')
        plt.bar(sm['year'], sm['total_deaths']/1e6, bottom= sm['total_deaths']/1e6, 
            color='red', label='women')
        plt.ylabel('Total deaths (millions)')
        plt.xlabel('Year')
        plt.legend()
        plt.show()
    
def testquery():
    Base = automap_base()
    
    passdict = password_ret(passfile=passfile)
    
    # connect(host, database username, password, database)
    db = 'testdb'
    
    engine = sqlalchemy.create_engine('mysql://'+passdict[db][0]+':'+passdict[db][1]+'@localhost/'+db, echo=False)   
    
    Session = sessionmaker()
    session = Session()
    
    # Reflect means grab all existing tables from engine (prepare them into same class as base)
    Base.prepare(engine, reflect=True)
    
    # REQUIRES primary key to select Base class
    #for mappedclass in Base.classes:
    #    print mappedclass
    
    #User = Base.classes.users #MUST HAVE PRIMARY KEY
    #a=session.query(User)
    
    ################## Query for total deaths by year ##################
    mort = Base.classes.mortall
    yeardeathall = session.query(mort.year.label('year'), func.sum(mort.num_deaths).label('total_deaths')).group_by(mort.year)
        
    ################## Query for total population by year ##################
    pop1 = Base.classes.pop6878
    pop2 = Base.classes.pop7988
    yearpop1 = session.query(pop1.year.label('year'), func.sum(pop1.pop1_4 + pop1.pop5_9 + pop1.pop10_14 +\
        pop1.pop15_19 + pop1.pop20_24 + pop1.pop25_34 + pop1.pop35_44 + pop1.pop45_54 + pop1.pop55_64 +\
        pop1.pop65_74 + pop1.pop75_84 + pop1.pop75_84).label('total_population')).group_by(pop1.year).filter(pop1.county_name == 'U.S.')
    yearpop2 = session.query(pop2.year.label('year'), func.sum(pop2.pop1_4 + pop2.pop5_9 + pop2.pop10_14 +\
        pop2.pop15_19 + pop2.pop20_24 + pop2.pop25_34 + pop2.pop35_44 + pop2.pop45_54 + pop2.pop55_64 +\
        pop2.pop65_74 + pop2.pop75_84 + pop2.pop75_84).label('total_population')).group_by(pop2.year).filter(pop2.county_name == 'U.S.')
    
    yearpopall = yearpop1.union_all(yearpop2)
    
    ################## Query for total deaths by year and sex ##################
    sexfdeath = session.query(mort.year.label('year'), 
        func.sum(mort.num_deaths).label('total_deaths')).group_by(mort.year).filter(mort.race_sex.in_([2,4,6]))
    sexmdeath = session.query(mort.year.label('year'), 
        func.sum(mort.num_deaths).label('total_deaths')).group_by(mort.year).filter(mort.race_sex.in_([1,3,5]))
    
    return yeardeathall, yearpopall, sexmdeath, sexfdeath
    
    
    