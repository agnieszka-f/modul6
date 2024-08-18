from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, ForeignKey

db_file = "db_taks-2.db"

meta = MetaData()

stations_columns = [
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
]

clean_measure_columns = [
    Column('date', String),
    Column('station', String, ForeignKey('stations.station')),
    Column('precip', Float),
    Column('tobs', Integer),
]

def create_db(file):
    engine = create_engine(f'sqlite:///{file}')
    return engine

def create_table(table_name, columns):
    return Table(
        table_name, meta,
        *columns
    )

def create_values(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        values = []
        first = next(file)
        headers = first.strip().split(',')
        
        for line in file:
            val = line.strip().split(',')
            row_dict = {headers[i]: val[i] for i in range(len(headers))}
            values.append(row_dict)
    
    return values
                

def insert_data(conn, ins, file_path):
    list_values = create_values(file_path)
    conn.execute(ins, list_values)

def select_all(table, limit=None):
    query = table.select()
    if limit is not None:
        query = query.limit(limit)
    return query

def print_data(data):
    for el in data:
        print(el)

if __name__ == "__main__":
    engine = create_db(db_file)
    stations = create_table('stations', stations_columns)
    clean_measure = create_table('clean_measure', clean_measure_columns)
    meta.create_all(engine)
    print(engine.table_names())
    conn = engine.connect()
    stations_ins = stations.insert()
    clean_measure_ins = clean_measure.insert()
    insert_data(conn, stations_ins, 'clean_stations.csv')
    insert_data(conn, clean_measure_ins, 'clean_measure.csv')
    all_stations = conn.execute(select_all(stations))
    print_data(all_stations)
    all_clean_measure = conn.execute(select_all(clean_measure))
    print_data(all_clean_measure)
    stations_limit_5 = conn.execute(select_all(stations, 5))
    
