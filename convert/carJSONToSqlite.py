import os
import json

from sqlalchemy import Integer, String, Column, Float, SmallInteger, create_engine
from sqlalchemy.orm import create_session
from sqlalchemy.ext.declarative import declarative_base


current_dir = os.path.dirname(__file__)
INPUT_PATH = os.path.join(current_dir, '..', 'json', 'car.json')
OUTPUT_PATH = os.path.join(current_dir, '..', 'sqlite', 'car.db')
Base = declarative_base()


class Car(Base):
    __tablename__ = 'car'
    sipid = Column(Integer, primary_key=True, nullable=False)
    lat = Column(Float)
    lon = Column(Float)
    carMake = Column(String(60))
    carModel = Column(String(60))
    carModelYear = Column(SmallInteger)
    timestamp = Column(String(14))


def create_db() -> None:
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    engine = create_engine(f'sqlite:///{OUTPUT_PATH}')
    session = create_session(bind=engine)
    Base.metadata.create_all(bind=engine)

    with open(INPUT_PATH, 'r') as f:
        data = json.loads(f.read())

    try:
        for entry in data:
            c = Car(**entry)
            session.add(c)
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    print('Success!')


if __name__ == '__main__':
    create_db()
