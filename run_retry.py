#!/usr/bin/env python3
"""
重试失败记录脚本
自动重试之前失败的请求
"""

from propertyguru_pipeline import PropertyGuruPipeline
import sys

def main():
    print("=" * 60)
    print("PropertyGuru 失败记录重试")
    print("=" * 60)

    try:
        # 创建Pipeline实例（使用10个线程）
        pipeline = PropertyGuruPipeline(max_workers=1)

        # 执行失败重试流程
        pipeline.retry_failed_records()

        print("\n✅ 失败记录重试完成！")
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
