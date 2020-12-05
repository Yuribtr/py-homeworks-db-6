from operator import and_
import sqlalchemy as sa
from inc import read_data, clear_db, Genre, Musician, Album, Track, Collection, read_query, Base, create_session
from sqlalchemy import func, distinct
from sqlalchemy.orm import sessionmaker

if __name__ == '__main__':
    DSN_SQL = 'postgresql://test:test@localhost:5432/TestSQL'
    DSN_ORM = 'postgresql://test:test@localhost:5432/TestORM'
    DATA = read_data('data/demo-data.csv')

    print('Connecting to DB\'s...')
    engine_orm = sa.create_engine(DSN_ORM)
    connection = engine_orm.connect()
    connection.close()
    Session_ORM = sessionmaker(bind=engine_orm)
    session_orm = Session_ORM()

    engine_sql = sa.create_engine(DSN_SQL)
    connection = engine_sql.connect()
    connection.close()
    Session_SQL = sessionmaker(bind=engine_sql)
    session_sql = Session_SQL()

    print('Clearing the bases...')
    clear_db(sa, engine_sql)
    clear_db(sa, engine_orm)

    print('Creating tables...')
    # we don't need to create tables for engine_sql, as they will be created later with SQL
    Base.metadata.create_all(engine_orm)

    print('\nPreparing data for ORM job...')
    for item in DATA:
        # creating genres
        genre = session_orm.query(Genre).filter_by(name=item['genre']).scalar()
        if not genre:
            genre = Genre(name=item['genre'])
        session_orm.add(genre)

        # creating musicians
        musician = session_orm.query(Musician).filter_by(name=item['musician']).scalar()
        if not musician:
            musician = Musician(name=item['musician'])
        musician.genres.append(genre)
        session_orm.add(musician)

        # creating albums
        album = session_orm.query(Album).filter_by(name=item['album']).scalar()
        if not album:
            album = Album(name=item['album'], year=item['album_year'])
        album.musicians.append(musician)
        session_orm.add(album)

        # creating tracks
        # checking track existence also by album because track field might be not unique
        track = session_orm.query(Track).join(Album).filter(and_(Track.name == item['track'],
                                                                 Album.name == item['album'])).scalar()
        if not track:
            track = Track(name=item['track'], length=item['length'])
        track.album_id = album.id
        session_orm.add(track)

        # creating collections, which can be empty
        if item['collection']:
            collection = session_orm.query(Collection).filter_by(name=item['collection']).scalar()
            if not collection:
                collection = Collection(name=item['collection'], year=item['collection_year'])
            collection.tracks.append(track)
            session_orm.add(collection)
        session_orm.commit()

    print('\nPreparing data for SQL job...')
    print('Creating empty tables...')
    session_sql.execute(read_query('queries/create-tables.sql'))
    session_sql.commit()

    print('\nAdding musicians...')
    query = read_query('queries/insert-musicians.sql')
    res = session_sql.execute(query.format(','.join({f"('{x['musician']}')" for x in DATA})))
    print(f'Inserted {res.rowcount} musicians.')

    print('\nAdding genres...')
    query = read_query('queries/insert-genres.sql')
    res = session_sql.execute(query.format(','.join({f"('{x['genre']}')" for x in DATA})))
    print(f'Inserted {res.rowcount} genres.')

    print('\nLinking musicians with genres...')
    # assume that musician + genre has to be unique
    genres_musicians = {x['musician'] + x['genre']: [x['musician'], x['genre']] for x in DATA}
    query = read_query('queries/insert-genre-musician.sql')
    # this query can't be run in batch, so execute one by one
    res = 0
    for key, value in genres_musicians.items():
        res += session_sql.execute(query.format(value[1], value[0])).rowcount
    print(f'Inserted {res} connections.')

    print('\nAdding albums...')
    # assume that albums has to be unique
    albums = {x['album']: x['album_year'] for x in DATA}
    query = read_query('queries/insert-albums.sql')
    res = session_sql.execute(query.format(','.join({f"('{x}', '{y}')" for x, y in albums.items()})))
    print(f'Inserted {res.rowcount} albums.')

    print('\nLinking musicians with albums...')
    # assume that musicians + album has to be unique
    albums_musicians = {x['musician'] + x['album']: [x['musician'], x['album']] for x in DATA}
    query = read_query('queries/insert-album-musician.sql')
    # this query can't be run in batch, so execute one by one
    res = 0
    for key, values in albums_musicians.items():
        res += session_sql.execute(query.format(values[1], values[0])).rowcount
    print(f'Inserted {res} connections.')

    print('\nAdding tracks...')
    query = read_query('queries/insert-track.sql')
    # this query can't be run in batch, so execute one by one
    res = 0
    for item in DATA:
        res += session_sql.execute(query.format(item['track'], item['length'], item['album'])).rowcount
    print(f'Inserted {res} tracks.')

    print('\nAdding collections...')
    query = read_query('queries/insert-collections.sql')
    res = session_sql.execute(query.format(','.join({f"('{x['collection']}', {x['collection_year']})" for x in DATA if
                                                     x['collection'] and x['collection_year']})))
    print(f'Inserted {res.rowcount} collections.')

    print('\nLinking collections with tracks...')
    query = read_query('queries/insert-collection-track.sql')
    # this query can't be run in batch, so execute one by one
    res = 0
    for item in DATA:
        res += session_sql.execute(query.format(item['collection'], item['track'])).rowcount
    print(f'Inserted {res} connections.')
    session_sql.commit()

    print('\nDatabase ready, let\'s have some fun!')

    print('\n1. All albums from 2018:')
    query = read_query('queries/select-album-by-year.sql').format(2018)
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Album).filter_by(year=2018):
        print(item.name)

    print('\n2. Longest track:')
    query = read_query('queries/select-longest-track.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Track).order_by(Track.length.desc()).slice(0, 1):
        print(f'{item.name}, {item.length}')

    print('\n3. Tracks with length not less 3.5min:')
    query = read_query('queries/select-tracks-over-length.sql').format(310)
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Track).filter(310 <= Track.length).order_by(Track.length.desc()):
        print(f'{item.name}, {item.length}')

    print('\n4. Collections between 2018 and 2020 years (inclusive):')
    query = read_query('queries/select-collections-by-year.sql').format(2018, 2020)
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Collection).filter(2018 <= Collection.year,
                                                     Collection.year <= 2020):
        print(item.name)

    print('\n5. Musicians with name that contains not more 1 word:')
    query = read_query('queries/select-musicians-by-name.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Musician).filter(Musician.name.notlike('%% %%')):
        print(item.name)

    print('\n6. Tracks that contains word "me" in name:')
    query = read_query('queries/select-tracks-by-name.sql').format('me')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Track).filter(Track.name.like('%%me%%')):
        print(item.name)

    print('Ok, let\'s start serious work')

    print('\n7. How many musicians plays in each genres:')
    query = read_query('queries/count-musicians-by-genres.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Genre).join(Genre.musicians).order_by(func.count(Musician.id).desc()).group_by(
            Genre.id):
        print(f'{item.name}, {len(item.musicians)}')

    print('\n8. How many tracks in all albums 2019-2020:')
    query = read_query('queries/count-tracks-in-albums-by-year.sql').format(2019, 2020)
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Track, Album).join(Album).filter(2019 <= Album.year, Album.year <= 2020):
        print(f'{item[0].name}, {item[1].year}')

    print('\n9. Average track length in each album:')
    query = read_query('queries/count-average-tracks-by-album.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Album, func.avg(Track.length)).join(Track).order_by(func.avg(Track.length)).group_by(
            Album.id):
        print(f'{item[0].name}, {item[1]}')

    print('\n10. All musicians that have no albums in 2020:')
    query = read_query('queries/select-musicians-by-album-year.sql').format(2020)
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Musician).join(Musician.albums).filter(Album.year != 2020).order_by(
            Musician.name.asc()):
        print(f'{item.name}')

    print('\n11. All collections with musician Steve:')
    query = read_query('queries/select-collection-by-musician.sql').format('Steve')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Collection).join(Collection.tracks).join(Album).join(Album.musicians).filter(
            Musician.name == 'Steve').order_by(Collection.name):
        print(f'{item.name}')

    print('\n12. Albums with musicians that play in more than 1 genre:')
    query = read_query('queries/select-albums-by-genres.sql').format(1)
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    for item in session_orm.query(Album).join(Album.musicians).join(Musician.genres).having(func.count(distinct(
            Genre.name)) > 1).group_by(Album.id).order_by(Album.name):
        print(f'{item.name}')

    print('\n13. Tracks that not included in any collections:')
    query = read_query('queries/select-absence-tracks-in-collections.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    # Important! Despite the warning, following expression does not work: "Collection.id is None"
    for item in session_orm.query(Track).outerjoin(Track.collections).filter(Collection.id == None):
        print(f'{item.name}')

    print('\n14. Musicians with shortest track length:')
    query = read_query('queries/select-musicians-min-track-length.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    subquery = session_orm.query(func.min(Track.length))
    for item in session_orm.query(Musician, Track.length).join(Musician.albums).join(Track).group_by(
            Musician.id, Track.length).having(Track.length == subquery).order_by(Musician.name):
        print(f'{item[0].name}, {item[1]}')

    print('\n15. Albums with minimum number of tracks:')
    query = read_query('queries/select-albums-with-minimum-tracks.sql')
    print(f'############################\n{query}\n############################')
    print('----SQL way---')
    res = session_sql.execute(query)
    print(*res, sep='\n')
    print('----ORM way----')
    subquery1 = session_orm.query(func.count(Track.id)).group_by(Track.album_id).order_by(func.count(Track.id)).limit(1)
    subquery2 = session_orm.query(Track.album_id).group_by(Track.album_id).having(func.count(Track.id) == subquery1)
    for item in session_orm.query(Album).join(Track).filter(Track.album_id.in_(subquery2)).order_by(Album.name):
        print(f'{item.name}')
