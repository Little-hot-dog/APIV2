from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


'''
Подключение к БД.
postgresql://{имя пользователя БД}:{пароль пользователя БД}@{ip БД}/{имя БД}
'''
SQl_DB_URL = "postgresql://postgres:12341234@192.168.1.162/mydatabase"

engine = create_engine(SQl_DB_URL, echo=True)
    #echo = True - все взаимодействия с БД будут отображаться в консоли

session_local = sessionmaker(autoflush=False, autocommit=False, bind=engine)

Base = declarative_base()