from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BIGINT, Boolean, Text, DateTime, BigInteger
from itemadapter import ItemAdapter
import datetime
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

class Fan(Base):
    """
    粉丝信息
    """
    __tablename__ = 'fans'
    # 自增主键
    _id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(20), primary_key=True)
    crawl_time = Column(BIGINT)
    follower_id = Column(String(20))
    avatar_hd = Column(String(255))
    nick_name = Column(String(100))
    verified = Column(Boolean)
    description = Column(Text)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    statuses_count = Column(Integer)
    gender = Column(String(1))
    location = Column(String(100))
    mbrank = Column(Integer)
    mbtype = Column(Integer)
    credit_score = Column(Integer)
    created_at = Column(String(50))

class WeiboPost(Base):
    """
    微博推文信息
    """
    __tablename__ = 'weibo_posts'

    _id = Column(BigInteger, primary_key=True)
    user_id = Column(String(20))
    crawl_time = Column(BIGINT)
    mblogid = Column(String(50), nullable=False)
    created_at = Column(DateTime)
    geo = Column(String(100))
    ip_location = Column(String(100))
    reposts_count = Column(Integer)
    comments_count = Column(Integer)
    attitudes_count = Column(Integer)
    source = Column(String(100))
    content = Column(Text)
    pic_urls = Column(Text)
    pic_num = Column(Integer)
    video = Column(String(250))
    video_online_numbers = Column(Integer)
    isLongText = Column(Boolean)
    is_retweet = Column(Boolean)
    retweet_id = Column(String(50))
    url = Column(String(250))

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
        if spider.name == 'user_spider':
            item['_id'] = item['id']
            entity = User(**item)
        if spider.name == 'fan':
            entity = Fan(
                user_id=item['fan_info']['_id'],
                crawl_time=item['crawl_time'],
                follower_id=item['follower_id'],
                avatar_hd=item['fan_info']['avatar_hd'],
                nick_name=item['fan_info']['nick_name'],
                verified=item['fan_info']['verified'],
                description=item['fan_info']['description'],
                followers_count=item['fan_info']['followers_count'],
                friends_count=item['fan_info']['friends_count'],
                statuses_count=item['fan_info']['statuses_count'],
                gender=item['fan_info']['gender'],
                location=item['fan_info']['location'],
                mbrank=item['fan_info']['mbrank'],
                mbtype=item['fan_info']['mbtype'],
                credit_score=item['fan_info'].get('credit_score', 0),
                created_at=item['fan_info']['created_at']
            )
        if spider.name == 'tweet_spider_by_user_id':
            entity = WeiboPost(**item)
        self.session.add(entity)
        self.session.commit()
        return item