from propertyguru_pipeline import PropertyGuruPipeline
import time
from loguru import logger

if __name__ == '__main__':
    # 创建Pipeline实例（可以根据需要调整线程数）
    pipeline = PropertyGuruPipeline(max_workers=10)

    # 执行失败重试流程
    start_time = time.time()
    pipeline.retry_failed_records()
    elapsed_time = time.time() - start_time
    
    logger.success(f"失败记录重试完成！总耗时: {elapsed_time:.2f} 秒")
