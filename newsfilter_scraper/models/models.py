# SQLAlchemy models for newsfilter.io data

from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

# Association tables for many-to-many relationships
article_symbols = Table(
    "article_symbols",
    Base.metadata,
    Column("article_id", String(255), ForeignKey("articles.id"), primary_key=True),
    Column("symbol_id", String(20), ForeignKey("symbols.symbol"), primary_key=True),
)

article_industries = Table(
    "article_industries",
    Base.metadata,
    Column("article_id", String(255), ForeignKey("articles.id"), primary_key=True),
    Column("industry_id", String(100), ForeignKey("industries.name"), primary_key=True),
)

article_sectors = Table(
    "article_sectors",
    Base.metadata,
    Column("article_id", String(255), ForeignKey("articles.id"), primary_key=True),
    Column("sector_id", String(100), ForeignKey("sectors.name"), primary_key=True),
)


class Source(Base):
    __tablename__ = "sources"

    id = Column(String(100), primary_key=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship to articles
    articles = relationship("Article", back_populates="source")

    def __repr__(self):
        return f"<Source(id='{self.id}', name='{self.name}')>"


class Symbol(Base):
    __tablename__ = "symbols"

    symbol = Column(String(20), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Many-to-many relationship with articles
    articles = relationship(
        "Article", secondary=article_symbols, back_populates="symbols"
    )

    def __repr__(self):
        return f"<Symbol(symbol='{self.symbol}')>"


class Industry(Base):
    __tablename__ = "industries"

    name = Column(String(100), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Many-to-many relationship with articles
    articles = relationship(
        "Article", secondary=article_industries, back_populates="industries"
    )

    def __repr__(self):
        return f"<Industry(name='{self.name}')>"


class Sector(Base):
    __tablename__ = "sectors"

    name = Column(String(100), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Many-to-many relationship with articles
    articles = relationship(
        "Article", secondary=article_sectors, back_populates="sectors"
    )

    def __repr__(self):
        return f"<Sector(name='{self.name}')>"


class Article(Base):
    __tablename__ = "articles"

    id = Column(String(255), primary_key=True)
    title = Column(Text, nullable=False)
    description = Column(Text)
    source_url = Column(Text, nullable=False)
    image_url = Column(Text, nullable=True)
    published_at = Column(DateTime, nullable=False)

    # Foreign key to source
    source_id = Column(String(100), ForeignKey("sources.id"), nullable=False)

    # Metadata fields
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    source = relationship("Source", back_populates="articles")
    symbols = relationship(
        "Symbol", secondary=article_symbols, back_populates="articles"
    )
    industries = relationship(
        "Industry", secondary=article_industries, back_populates="articles"
    )
    sectors = relationship(
        "Sector", secondary=article_sectors, back_populates="articles"
    )

    def __repr__(self):
        return f"<Article(id='{self.id}', title='{self.title[:50]}...', published_at='{self.published_at}')>"
