import sqlalchemy as sa
from sqlalchemy import ForeignKey, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Genre(Base):
    __tablename__ = 'genres'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(20), unique=True)
    musicians = relationship("Musician", secondary='genres_musicians')


class Musician(Base):
    __tablename__ = 'musicians'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(50), unique=True)
    # relations many to many with two tables
    genres = relationship("Genre", secondary='genres_musicians')
    albums = relationship("Album", secondary='albums_musicians')


class GenreMusician(Base):
    __tablename__ = 'genres_musicians'
    __table_args__ = (PrimaryKeyConstraint('genre_id', 'musician_id'),)
    genre_id = sa.Column(sa.Integer, sa.ForeignKey('genres.id'))
    musician_id = sa.Column(sa.Integer, sa.ForeignKey('musicians.id'))


class Album(Base):
    __tablename__ = 'albums'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(50), unique=True)
    year = sa.Column(sa.Integer)
    musicians = relationship("Musician", secondary='albums_musicians')


class AlbumMusician(Base):
    __tablename__ = 'albums_musicians'
    __table_args__ = (PrimaryKeyConstraint('album_id', 'musician_id'),)
    album_id = sa.Column(sa.Integer, sa.ForeignKey('albums.id'))
    musician_id = sa.Column(sa.Integer, sa.ForeignKey('musicians.id'))


class Track(Base):
    __tablename__ = 'tracks'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(100))
    length = sa.Column(sa.Integer)
    # relation one to many
    album_id = sa.Column(sa.Integer, ForeignKey('albums.id'))
    collections = relationship("Collection", secondary='collections_tracks')


class Collection(Base):
    __tablename__ = 'collections'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(50))
    year = sa.Column(sa.Integer)
    tracks = relationship("Track", secondary='collections_tracks')


class CollectionTrack(Base):
    __tablename__ = 'collections_tracks'
    __table_args__ = (PrimaryKeyConstraint('collection_id', 'track_id'),)
    collection_id = sa.Column(sa.Integer, sa.ForeignKey('collections.id'))
    track_id = sa.Column(sa.Integer, sa.ForeignKey('tracks.id'))
