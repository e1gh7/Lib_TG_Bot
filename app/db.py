from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm import relationship
import asyncio
import aiohttp

Books_DB = "postgresql://e1gh7:3307@localhost:5432/BOOKS_DB"
engine = create_engine(Books_DB)
BASE_URL = "https://openlibrary.org/subjects/{}.json?limit=100"


genres = {
    "architecture", "art history", "dance", "design", "fashion", "film", "graphic design",
    "music", "music theory", "painting", "photography", "animals", "bears", "cats", "kittens",
    "dogs", "puppies", "fiction", "fantasy", "historical fiction", "horror", "humor",
    "literature", "magic", "mystery and detective stories", "plays", "poetry", "romance",
    "science fiction", "short stories", "thriller", "young adult", "biology", "chemistry",
    "mathematics", "physics", "programming", "management", "entrepreneurship", "finance",
    "stories in rhyme", "baby books", "picture books", "history", "ancient civilization",
    "archaeology", "anthropology", "world war ii", "social life and customs", "cooking",
    "cookbooks", "mental health", "exercise", "nutrition", "self help", "biography",
    "autobiographies", "history", "politics and government", "women", "kings and rulers",
    "composers", "artists", "social sciences", "political science", "psychology", "brazil",
    "india", "indonesia", "united states", "textbooks", "mathematics", "geography",
    "english language", "computer science", "english", "french", "spanish", "german",
    "russian", "italian", "chinese", "japanese"
}


class Base(DeclarativeBase): pass

class Book(Base):
    __tablename__ = "books"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    genre_id = Column(Integer, ForeignKey("genres.id"))
    genre = relationship("Genre", back_populates="books")

class Genre(Base):
    __tablename__ = "genres"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    books = relationship("Book", back_populates="genre", cascade="all, delete-orphan")

Base.metadata.create_all(bind=engine)

def put_genre_and_books(books, genre):
    with Session(autoflush=False, bind=engine) as db:
            genre = Genre(name=genre)
            db.add(genre)
            db.commit()
            db.refresh(genre)

    for book in books:
            book = Book(name=book, genre_id=genre.id)
            db.add(book)
    db.commit()

semaphore = asyncio.Semaphore(10)

async def fetch(session, url, genre):
    async with semaphore:
          async with session.get(url) as response:
            data = await response.json()
            titles = [data['works'][count]['title'] for count in range(100)]
            put_genre_and_books(titles, genre)
async def main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[fetch(session, BASE_URL.format(genre), genre) for genre in genres])

asyncio.run(main())






