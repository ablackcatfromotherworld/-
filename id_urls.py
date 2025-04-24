import motor.motor_asyncio
import aiomysql
import asyncio
import json
import platform
import time

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 配置参数
BATCH_SIZE = 500  # 批量处理大小
PARALLEL_TASKS = 5  # 并行任务数量


# 主异步函数，用于设置连接
async def main():
    start_time = time.time()
    print(f"开始执行，时间: {time.strftime('%H:%M:%S', time.localtime(start_time))}")

    # 连接到 MongoDB
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://wnhmsy:wnhmsy@43.157.146.240:27017/')
    db = client['spiders']
    movies_collection = db['movies3']

    # 创建 MySQL 连接池
    pool = await aiomysql.create_pool(
        host='127.0.0.1',
        user='spiderman',
        password='ew4%98fRpe',
        port=3306,
        db='spider',
        charset='utf8mb4',
        autocommit=True,
        maxsize=10  # 增加连接池大小
    )
    print("MySQL 连接池创建成功")
    try:
        # 调用处理函数，并传递连接对象
        await get_id_urls(client, db, movies_collection, pool)
    finally:
        # 在主函数结束时关闭连接
        pool.close()
        await pool.wait_closed()
        client.close()
        end_time = time.time()
        print(f"所有连接已关闭，总耗时: {end_time - start_time:.2f} 秒")


# 处理数据的函数，接收连接对象作为参数
async def get_id_urls(client, db, movies_collection, pool):
    try:
        # 首先从MySQL获取已存在的所有ID到内存中
        existing_ids = set()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM id_urls_subtitles")
                async for row in cur:
                    existing_ids.add(row[0])

        print(f"从MySQL获取到 {len(existing_ids)} 个已存在ID")

        # 查询MongoDB中成功的电影总数
        total_movies = await movies_collection.count_documents({'status': 'success'})
        print(f"MongoDB中共有 {total_movies} 个状态为success的电影")

        # 使用批处理和并行处理来优化
        count = 0
        duplicate_count = 0
        batch_count = 0

        # 创建批处理函数
        async def process_batch(batch):
            nonlocal count, duplicate_count

            # 过滤掉已存在的ID
            new_records = []
            for movie in batch:
                id_ = movie['id']
                if id_ in existing_ids:
                    duplicate_count += 1
                    continue

                urls = {
                    480: movie['streams'].get('480p', {}).get('480p_cos_path', ''),
                    720: movie['streams'].get('720p', {}).get('720p_cos_path', ''),
                    1080: movie['streams'].get('1080p', {}).get('1080p_cos_path', '')
                }

                new_records.append((id_, json.dumps(urls)))
                existing_ids.add(id_)  # 添加到已存在集合中，避免后续重复插入

            if not new_records:
                return

            # 批量插入MySQL
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    try:
                        await cur.executemany(
                            "INSERT INTO id_urls_subtitles (id, urls) VALUES (%s, %s)",
                            new_records
                        )
                        count += len(new_records)
                    except aiomysql.Error as e:
                        print(f"批量插入时出错: {e}")

        # 分批处理
        current_batch = []

        async for movie in movies_collection.find({'status': 'success'}):
            current_batch.append(movie)

            if len(current_batch) >= BATCH_SIZE:
                batch_count += 1
                print(f"处理第 {batch_count} 批，{len(current_batch)} 条记录")
                await process_batch(current_batch)
                current_batch = []
                print(f"已处理 {count} 条记录，跳过 {duplicate_count} 条重复记录")

        # 处理最后一批
        if current_batch:
            batch_count += 1
            print(f"处理最后一批（第 {batch_count} 批），{len(current_batch)} 条记录")
            await process_batch(current_batch)

        print(f"总共成功插入 {count} 条记录，跳过 {duplicate_count} 条重复记录")

    except Exception as e:
        print(f"发生错误: {e}")


if __name__ == '__main__':
    asyncio.run(main())