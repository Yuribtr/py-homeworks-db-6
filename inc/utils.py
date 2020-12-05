import csv

from sqlalchemy.orm import sessionmaker


def read_query(filename):
    """
    Reads query from SQL text file
    :param filename: name of file with query
    :return: text of query
    """
    query_file = open(filename, mode='rt', encoding='utf-8')
    query_text = ''.join(query_file.readlines())
    query_file.close()
    return query_text


def read_data(filename, delimiter=';'):
    """
    Parse CSV data and return as dictionary
    :param filename: name of sample data in CSV
    :param delimiter: delimeter, used in CSV file
    :return: dictionary with all found elements in file
    """
    data_file = open(filename, mode='rt', encoding='utf-8')
    csv.register_dialect('MyDialect', delimiter=delimiter)
    tmp = []
    reader = csv.DictReader(data_file, dialect='MyDialect')
    for item in reader:
        tmp.append(item)
    return tmp


def clear_db(sqlalchemy, engine):
    """
    Drop all tables in DB
    :param sqlalchemy: sqlalchemy instance
    :param engine: db engine
    :return: none
    """
    inspect = sqlalchemy.inspect(engine)
    for table_entry in reversed(inspect.get_sorted_table_and_fkc_names()):
        table_name = table_entry[0]
        if table_name:
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text(f'DROP TABLE "{table_name}"'))
    return


def create_session(sqlalchemy, dsn: str) -> sessionmaker:
    engine = sqlalchemy.create_engine(dsn)
    Session = sessionmaker(bind=engine)
    return Session()
