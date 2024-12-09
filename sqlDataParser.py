import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError


#Helper class to parse panda dataframes
class DataParser:

    def __init__(self, username:str, password:str,host:str,port:int,databaseName:str):
        self.sqlEngine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{databaseName}")
        #Test database connection
        try:
            with self.sqlEngine.connect() as connection:
                connection.execute(text("SELECT 1"))
                print(f"Successful connection to Database.")
        except OperationalError as e:
            print(f"Error connecting to the database: {e}")
        
        self.Session = sessionmaker(bind=self.sqlEngine)

    #parse data, rollback everything if fail
    def parsePandaDFToTable(self,dataframe:pd.DataFrame,tableName):
        session = self.Session()
        #make whole parse atomic
        try:
            #Insert dataframe
            dataframe.to_sql(tableName, session.bind, if_exists='append', index=False)
            session.commit()
        except Exception as e:
            session.rollback()
            print("Transaction failed:", e)
        finally:
            session.close()
            
    #Make normal call
    def makeCall(self, query):
        with self.sqlEngine.connect() as connection:
            return connection.execute(text(query)).fetchall()


    def makeCall(self, query, params=None):
        if params is None:
            params = {}
        with self.sqlEngine.connect() as connection:
            return connection.execute(text(query), params).fetchall()
