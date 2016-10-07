import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
import matplotlib.pyplot as plt

passfile = '/Users/vickitoy/sideprojects/mortality/password.txt'

def password_ret(passfile=passfile):
    
    passdict = {}
    with open(passfile, 'r') as f:
        for line in f:
            idb, iuser, ipass = line.split(',')
            passdict[idb] = (iuser, ipass)
            
    return passdict


def totaldeaths():

    passdict = password_ret(passfile=passfile)
    
    # connect(host, database username, password, database)
    db = 'testdb'
    
    engine = sqlalchemy.create_engine('mysql://'+passdict[db][0]+':'+passdict[db][1]+'@localhost/'+db, echo=False)    
    conn=engine.connect()
    
    #a=pd.read_sql('SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort6878 GROUP BY year UNION SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort7988 GROUP BY year;', conn)    
    #a.plot('year', 'total_deaths')
    #plt.savefig()
    
    a = testquery()
    
    tester = pd.read_sql(a.statement, conn)
    #men=pd.read_sql('SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort6878 WHERE race_sex IN (1,3,5) GROUP BY year UNION ALL SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort7988 WHERE race_sex IN (1,3,5) GROUP BY year;', conn) 
    
    #women=pd.read_sql('SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort6878 WHERE race_sex IN (2,4,6) GROUP BY year UNION ALL SELECT year, SUM(num_deaths)/1000000 as total_deaths FROM mort7988 WHERE race_sex IN (2,4,6) GROUP BY year;', conn) 
    
    #women.plot('year', 'total_deaths')
    #plt.title('women deaths in millions')
    #plt.savefig('womentotaldeaths.png')
    
    print tester
    
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
    
    mort1 = Base.classes.mort6878
    mort2 = Base.classes.mort7988
    a = session.query(mort1.year, func.sum(mort1.num_deaths).label('total_deaths')).group_by(mort1.year)
    #b = session.query(mort2)
    
    #c = a.union(b)
    print a.statement
    return a
    