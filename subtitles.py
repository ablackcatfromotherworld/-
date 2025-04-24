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
        maxsize=10
    )
    print("MySQL 连接池创建成功")

    try:
        # 调用处理函数，并传递连接对象
        await update_subtitles(client, db, movies_collection, pool)
    finally:
        # 在主函数结束时关闭连接
        pool.close()
        await pool.wait_closed()
        client.close()
        end_time = time.time()
        print(f"所有连接已关闭，总耗时: {end_time - start_time:.2f} 秒")


async def update_subtitles(client, db, movies_collection, pool):
    try:
        # 第一步：从MySQL的id_urls_subtitles表中读取所有ID
        all_ids = []
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 这里假设表中有id字段
                await cur.execute("SELECT id FROM id_urls_subtitles")
                async for row in cur:
                    all_ids.append(row[0])

        if not all_ids:
            print("MySQL表中没有找到ID，程序结束")
            return

        total_ids = len(all_ids)
        print(f"从MySQL中读取到 {total_ids} 个需要处理的ID")

        # 统计数据
        processed_count = 0
        updated_count = 0
        not_found_count = 0
        error_count = 0
        batch_count = 0

        # 分批处理ID
        for i in range(0, total_ids, BATCH_SIZE):
            batch_ids = all_ids[i:i + BATCH_SIZE]
            batch_count += 1
            print(f"处理第 {batch_count} 批，包含 {len(batch_ids)} 个ID")

            # 创建ID到字幕信息的映射
            id_to_subtitles = {}

            # 查询MongoDB获取字幕信息
            async for movie in movies_collection.find({'id': {'$in': batch_ids}}):
                if 'subtitles' in movie and movie['subtitles']:
                    # 提取并简化字幕信息
                    simplified_subtitles = [
                        {
                            'language': item.get('language', ''),
                            'subtitle_cos_path': item.get('subtitle_cos_path', '')
                        }
                        for item in movie['subtitles']
                        if 'language' in item and 'subtitle_cos_path' in item
                    ]

                    if simplified_subtitles:
                        id_to_subtitles[movie['id']] = json.dumps(simplified_subtitles)

            # 更新MySQL表中的字幕信息
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for movie_id in batch_ids:
                        processed_count += 1

                        try:
                            if movie_id in id_to_subtitles:
                                # 更新字幕信息
                                await cur.execute(
                                    "UPDATE id_urls_subtitles SET subtitles = %s WHERE id = %s",
                                    (id_to_subtitles[movie_id], movie_id)
                                )

                                if cur.rowcount > 0:
                                    updated_count += 1
                                else:
                                    print(f"警告：ID {movie_id} 的记录未能更新")
                            else:
                                not_found_count += 1
                                if not_found_count <= 10:
                                    print(f"在MongoDB中未找到ID: {movie_id} 的字幕信息")
                        except Exception as e:
                            error_count += 1
                            print(f"处理ID {movie_id} 时出错: {e}")

            # 显示当前批次的进度
            progress = (processed_count / total_ids) * 100
            print(f"进度: {progress:.2f}% ({processed_count}/{total_ids}), "
                  f"已更新: {updated_count}, 未找到: {not_found_count}, 错误: {error_count}")

        # 显示最终统计信息
        print("\n" + "=" * 50)
        print("处理完成，最终统计：")
        print(f"总处理ID数: {processed_count}")
        print(f"成功更新数: {updated_count}")
        print(f"MongoDB中未找到数: {not_found_count}")
        print(f"处理出错数: {error_count}")
        print("=" * 50)

    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())