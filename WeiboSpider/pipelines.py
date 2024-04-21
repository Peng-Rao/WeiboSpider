from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BIGINT, Boolean, Text
from itemadapter import ItemAdapter
import time

Base = declarative_base()

class User(Base):
    """
    用户信息
    """
    __tablename__ = 'user'
    _id = Column(String(20), primary_key=True)
    crawl_time = Column(BIGINT)
    avatar_hd = Column(String(255))
    nick_name = Column(String(100))
    verified = Column(Boolean)
    description = Column(Text)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    statuses_count = Column(Integer)
    gender = Column(String(5))
    location = Column(String(100))
    mbrank = Column(Integer)
    mbtype = Column(Integer)
    verified_type = Column(Integer)
    verified_reason = Column(Text)
    birthday = Column(String(50))
    created_at = Column(String(50))
    desc_text = Column(Text)
    ip_location = Column(String(100))
    sunshine_credit = Column(String(100))
    label_desc = Column(Text)
    company = Column(String(100))
    education = Column(String(100))


engine = create_engine('mysql+mysqlconnector://root:123456@localhost:3306/zyq')
Base.metadata.create_all(engine)  # 创建表格，如果表已存在，则忽略
Session = sessionmaker(bind=engine)


class SQLAlchemyPipeline:
    def open_spider(self, spider):
        # 创建数据库 session
        self.session = Session()

    def close_spider(self, spider):
        # 关闭 session
        self.session.close()

    def process_item(self, item, spider):
        item['crawl_time'] = int(time.time())
        user = User(**item)
        self.session.add(user)
        self.session.commit()
        return item