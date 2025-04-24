import motor.motor_asyncio
import aiomysql
import asyncio
import json
import platform
import time

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy)

BATCH_SIZE = 500
PARALLEL_TASKS = 5

async def main():
    start_time = time.time()
    print(f"开始时间：{time.strftime('%H:%M:%S', time.localtime(start_time))}")

    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://wnhmsy:wnhmsy@43.157.146.240:27017/')
    db = client['spiders']
    movie_collection = db['movies3']

    pool = await aiomysql.create_pool(
        host='127.0.0.1',
        user='spiderman',
        passward='ew4%98fRpe',
        port=3306,
        db='spider',
        charset='utf8mb4',
        autocommit=True,
        maxsize=10
    )

    try:
        await get_id_urls(client, db, movie_collection, pool)
    finally:
        pool.close()
        await pool.wait_closed()
        client.close()
        end_time = time.time()
        print(f'花费时间：{end_time - start_time:.2f}')

async def get_id_urls(client, db, movies_collection, pool):
    try:
        existing_ids = set()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM test")
                async for row in cur:
                    existing_ids.add(row[0])

        print(f"已存在的id数量：{len(existing_ids)}")

        total_movies = await movies_collection.count_documents({'status': 'success'})
        print(f"MongoDB中共有 {total_movies} 个状态为success的电影")

        count = 0
        duplicate_count = 0
        batch_count = 0

        async def process_batch(batch):
            nonlocal count, duplicate_count

            new_records = []
            for movie in batch:
                id_ = movie['id']
                if id_ in existing_ids:
                    duplicate_count += 1
                    # continue

                    subtitles = movie['subtitles']
                    simplified_subtitles = [{'language': item['language'], 'subtitle_cos_path': item['subtitle_cos_path']} for item in subtitles]
                    new_records.append((json.dumps(simplified_subtitles)))
                    # existing_ids.add(id)

            if not new_records:
                return

            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    try:
                        await cur.executemany("INSERT INTO id_urls_subtitles (subtitles) value (%s)", new_records)
                        count += len(new_records)
                    except aiomysql.Error as e:
                        print(f"批量插入时出错: {e}")

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
