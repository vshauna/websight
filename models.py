import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, UniqueConstraint, CheckConstraint, Index
import sqlalchemy.types
from sqlalchemy.types import Integer, String, Numeric, Boolean, DateTime
from sqlalchemy.orm import sessionmaker, relationship, backref
from datetime import datetime, timedelta
from decimal import Decimal as D
from sqlalchemy import func
from sqlalchemy.dialects import mysql
import urllib.parse
import settings


engine = sqlalchemy.create_engine(
    'mysql+mysqldb://{}:{}@{}/{}',
    settings.DATABASE_USERNAME,
    settings.DATABASE_PASSWORD,
    settings.DATABASE_HOST,
    settings.DATABASE_NAME,
    echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

'''
class SqliteNumeric(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.types.String
    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(sqlalchemy.types.VARCHAR(100))
    def process_bind_param(self, value, dialect):
        return str(value)
    def process_result_value(self, value, dialect):
        return D(value)

Numeric = SqliteNumeric
'''

Decimal = mysql.DECIMAL
class CategorySnapshot(Base):
    __tablename__ = 'category_snapshot'
    __table_args__ = (UniqueConstraint('source', 'parent_category_snapshot_id', 'name'),)

    id = Column(Integer, primary_key=True)
    parent_category_snapshot_id = Column(Integer, ForeignKey('category_snapshot.id'))
    name = Column(String(255), nullable=False)
    source = Column(String(255), nullable=False)
    source_id = Column(String(255))
    source_url = Column(String(2083))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    children = relationship('CategorySnapshot',
                backref=backref('parent', remote_side=[id]))

    def __repr__(self):
        return '<CategorySnapshot: {}: {} from {}>'.format(self.id, self.name, self.source)

    def descendants(self, l=[]):
        if not self.parent_id:
            l = []
        for child in self.children:
            l.append(child)
            child.descendants(l)
        return l

    def products(self, after_date=None):
        descendants = self.descendants() + [self]
        descendants_ids = [descendant.id for descendant in descendants]
        if after_date:
            return session.query(Product) \
                          .filter(Product.created_at >= after_date) \
                          .filter(Product.category.in_(descendants_ids)) \
                          .all()
        else:
            return session.query(Product) \
                          .filter(Product.category.in_(descendants_ids)) \
                          .all()

    @staticmethod
    def roots():
        categories = session.query(CategorySnapshot).all()
        roots = []
        for category in categories:
            if not category.parent_id:
                roots.append(category)
        return roots

class ProductSnapshot(Base):
    __tablename__ = 'product_snapshot'
    __table_args__ = (UniqueConstraint('source_id', 'price', 'available'),)

    id = Column(Integer, primary_key=True)
    category_snapshot_id = Column(Integer, ForeignKey('category_snapshot.id'))
    source_id = Column(String(255))
    name = Column(String(255), nullable=False)
    currency = Column(String(length=5), default=settings.DEFAULT_CURRENCY, nullable=False)
    source_url = Column(String(2083))
    price = Column(Decimal(27, 12), nullable=False)
    available = Column(Boolean, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    unavailable_at = Column(DateTime, nullable=True)
    type = Column(String(255))

    __mapper_args__ = {
        'polymorphic_identity': 'product_snapshot',
        'polymorphic_on': type,
    }

    def __repr__(self):
        return '<Product: {}: {} @ {:,}>'.format(self.id, self.name, int(self.price))

class SexProviderSnapshot(ProductSnapshot):
    __tablename__ = 'sex_provider_snapshot'
    __table_args__ = (CheckConstraint("gender IN ('woman', 'man', 'transwoman', 'transman')"),)

    id = Column(Integer, ForeignKey('product_snapshot.id'), primary_key=True)
    phone_number = Column(String(255))
    gender = Column(String(16), nullable=False)
    age = Column(Integer)
    height = Column(Decimal(4, 3))
    weight = Column(Decimal(6, 3))
    pic_url = Column(String(2083))
    pic_filename = Column(String(32767))

    __mapper_args__ = {
        'polymorphic_identity': 'sex_provider_snapshot',
    }

    @property
    def full_pic_url(self):
        return urllib.parse.urljoin(settings.BASE_URL, self.pic_url)

    def for_public(self):
        return {'name': self.name,
                'phone_number': self.phone_number,
                'price': str(self.price),
                'gender': self.gender,
                'age': self.age,
                'height': str(self.height),
                'weight': str(self.weight),
                'source_url': self.source_url,
                'pic_url': self.full_pic_url}

class Trade(Base):
    __tablename__ = 'trade'
    id = Column(Integer, primary_key=True)
    source = Column(String(255), nullable=False)
    source_id = Column(String(255), nullable=False)
    date = Column(DateTime, nullable=False, index=True)
    price = Column(Decimal(27, 12), nullable=False)
    amount = Column(Decimal(27, 12), nullable=False)
    price_currency = Column(String(length=5), default=settings.DEFAULT_CURRENCY, nullable=False)
    amount_currency = Column(String(length=5), default='BTC', nullable=False)

    @staticmethod
    def volume(trades):
        return sum(trade.amount for trade in trades)

    @staticmethod
    def mean(trades):
        volume = Trade.volume(trades)
        return sum(trade.amount/volume*trade.price for trade in trades)

    @staticmethod
    def mean_time(minutes=60):
        trades  = session.query(Trade).filter(Trade.date> datetime.utcnow()-timedelta(minutes=minutes)).all()
        return Trade.mean(trades)
        
    @staticmethod
    def volume_time(minutes=60):
        trades  = session.query(Trade).filter(Trade.date > datetime.utcnow()-timedelta(minutes=minutes)).all()
        return Trade.volume(trades)

    @staticmethod
    def last_trades(k=100, result=None):
        if result:
            trades = session.query(*result).order_by(Trade.date.desc()).limit(k).all()
        else:
            trades = session.query(Trade).order_by(Trade.date.desc()).limit(k).all()
        trades.reverse()
        return trades

    @staticmethod
    def last_trades_time(since_date, result=None):
        if result:
            trades = session.query(*result).filter(Trade.date > since_date).order_by(Trade.date.asc()).all()
        else:
            trades = session.query(Trade).filter(Trade.date > since_date).order_by(Trade.date.asc()).all()
        return trades

    @staticmethod
    def aggregate_trades_rows(trades, by='day', summary=True):
        '''
            these rows have form (date, amount, price)
        '''
        data = {}
        if by == 'minute':
            for row in trades:
                if row[0].date() in data:
                    data[row[0].replace(second=0, microsecond=0)]['trades'].append((row[1], row[2]))
                else:
                    data[row[0].replace(second=0, microsecond=0)] = {'trades': [(row[1], row[2])]}
        elif by == 'hour':
            for row in trades:
                if row[0].date() in data:
                    data[row[0].replace(minute=0, second=0, microsecond=0)]['trades'].append((row[1], row[2]))
                else:
                    data[row[0].replace(minute=0, second=0, microsecond=0)] = {'trades': [(row[1], row[2])]}
        elif by == 'day':
            for row in trades:
                if row[0].date() in data:
                    data[row[0].date()]['trades'].append((row[1], row[2]))
                else:
                    data[row[0].date()] = {'trades': [(row[1], row[2])]}
        else:
            raise Exception('invalid by parameter: {}'.format(by))

        for key in data:
            volume = sum([x[0] for x in data[key]['trades']])
            data[key]['mean_price'] = sum(trade[0]/volume*trade[1] for trade in data[key]['trades'])
            data[key]['volume'] = volume
            if summary:
                data[key].pop('trades')
            
        return data

    def __repr__(self):
        return '<Trade {}: {}{}/{}*{}{}={}{}>'.format(self.date, self.price, self.price_currency, self.amount_currency, self.amount, self.amount_currency, round(self.price*self.amount, 2), self.price_currency)

class ExchangeRateSnapshot(Base):
    __tablename__ = 'exchange_rate_snapshot'
    __table_args__ = (Index('idx_name_source_from_to', 'name', 'source', 'from_currency', 'to_currency'),)

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    source = Column(String(255), nullable=False)
    from_currency = Column(String(length=5), nullable=False, index=True)
    to_currency = Column(String(length=5), nullable=False, index=True)
    price = Column(Decimal(27, 12), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    def __repr__(self):
        return '<ExchangeRateSnapshot: {} {}/{}>'.format(self.price, self.from_currency, int(self.to_currency))

    @staticmethod
    def latest_price(name, source, from_currency, to_currency):
        max_date_query = session.query(func.max(ExchangeRateSnapshot.created_at)) \
                                .filter(ExchangeRateSnapshot.name == name) \
                                .filter(ExchangeRateSnapshot.source == source) \
                                .filter(ExchangeRateSnapshot.from_currency == from_currency) \
                                .filter(ExchangeRateSnapshot.to_currency == to_currency)
        last_price = session.query(ExchangeRateSnapshot.price) \
                            .filter(ExchangeRateSnapshot.name == name) \
                            .filter(ExchangeRateSnapshot.source == source) \
                            .filter(ExchangeRateSnapshot.from_currency == from_currency) \
                            .filter(ExchangeRateSnapshot.to_currency == to_currency) \
                            .filter(ExchangeRateSnapshot.created_at == max_date_query) \
                            .first()
        if last_price:
            last_price = last_price[0]
        return last_price
    
if __name__ == '__main__':
    Base.metadata.create_all(engine)
