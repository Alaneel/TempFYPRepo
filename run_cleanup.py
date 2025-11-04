#!/usr/bin/env python3
"""
定期清理过期数据脚本
建议每周运行一次，清理超过30天未更新的listings
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 数据清理")
    print("=" * 60)

    try:
        # 创建Pipeline实例
        pipeline = PropertyGuruPipeline(max_workers=5)

        # 清理超过30天未更新的listings（网站默认保留1个月）
        pipeline.cleanup_expired_data(days=30)

        print("\n✅ 数据清理完成！")
        return 0

    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())

