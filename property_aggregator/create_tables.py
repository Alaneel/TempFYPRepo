from .database import engine, Base
from .models import Listing


def main():
    print("正在连接到数据库并创建表...")
    
    # Base.metadata.create_all 是一个关键函数，
    # 它会检查数据库中是否存在名为 'listings' 的表，
    # 如果不存在，它将根据我们在 models.py 中的定义创建它。
    # 如果表已存在，它不会进行任何操作。
    Base.metadata.create_all(bind=engine)
    
    print("操作完成。如果 'listings' 表不存在，则已成功创建。")

if __name__ == "__main__":
    main()
