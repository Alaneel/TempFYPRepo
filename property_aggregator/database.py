from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- 数据库连接配置 ---
# 请将下面的字符串替换为您的真实 PostgreSQL 连接信息
# 格式: "postgresql://<用户名>:<密码>@<主机地址>:<端口>/<数据库名>"
# 示例: "postgresql://user:password@localhost:5432/real_estate_db"
DATABASE_URL = "postgresql://alanwang:!Wym20031009@localhost:5432/real_estate_db"


# 创建数据库引擎
engine = create_engine(DATABASE_URL)

# 创建一个SessionLocal类，用于数据库会话
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建一个Base类，我们的ORM模型将继承这个类
Base = declarative_base()
