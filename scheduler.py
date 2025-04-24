import asyncio
import copy
import json
import random
import shutil
import time
import traceback
from pathlib import Path
from typing import Optional, List, Type

import aioredis

from common.dbutils import get_aioredis_client
from common.errors import DownloadError, SkipAdError, UploadError, NoSpaceError, EmptyM3U8Error, ForbiddenError
from common.log import get_logger
from common.orm import scope_session, V4Mappings
from common.status import Status
from common.utils import silent
from core.database import DataBaseORM
from core.task import AsyncTask
from core.task_manager import TaskManager


class Scheduler:

    def __init__(self, settings):
        self.settings = settings
        self.logger = get_logger(settings.get("LOG_LEVEL", "INFO"))
        self.redis_client = get_aioredis_client(settings.get('REDIS_URI'))
        self.task_manager = TaskManager(concurrency=settings.get('CONCURRENCY_TASK'))
        self.orm = DataBaseORM(settings)
        self.redis_key = settings.get('DOWNLOAD_KEY', 'download_ts_eid')
        self.running_tasks = set()

    async def run(self):
        while True:
            ntasks = len(self.task_manager.current_task)
            if (eid := await self.get_next_request()) is not None:
                self.logger.info(f"当前有个{ntasks}任务")
                await self.task_manager.create_task(self.process_item(eid))
                await asyncio.sleep(random.uniform(0, 1))
            else:
                if ntasks < self.settings.get('CONCURRENCY_TASK'):
                    self.logger.info(f"等待任务")
                    await asyncio.sleep(2)

    async def process_item(self, eid):
        if eid not in self.running_tasks:
            self.running_tasks.add(eid)
            if videos := self.orm.query_offical_videos(eid):
                tasks = []
                for video in videos:
                    tasks.append(self.process_video(video))
                if tasks:
                    await asyncio.gather(*tasks)
            else:
                self.logger.warning(f"[{eid}] can not find official video")
            self.running_tasks.remove(eid)
        else:
            self.logger.info(f"[{eid}] 已经在下载了")

    async def process_video(self, video):
        self.logger.info(f"准备下载播放链接[{video.id}][{video.purl}]")
        self.orm.change_download_info(video.id, status=Status.Running.value, cid=video.cid, object_prefix=None, eid=video.eid)
        status, object_prefix = await self.run_task(video)
        self.orm.change_download_info(video.id, status=status.value, cid=video.cid, object_prefix=object_prefix, eid=video.eid)

    async def run_task(self, video):
        folder = self.settings.get('DOWNLOAD_DIR')
        root = Path(folder) / f'v{str(video.id)}'
        status = Status.Running
        self.clean(video, root.absolute(), status)
        object_prefix = f"v{video.id}/"
        try:
            object_prefix = await AsyncTask(self.settings, video).run()
            status = Status.Success
        except DownloadError:
            status = Status.DownloadFailed
            self.logger.error(f"[{video.id}][{video.eid}] - 下载失败 - {traceback.format_exc()}")
        except SkipAdError:
            status = Status.SkipAdFailed
            self.logger.error(f"[{video.id}][{video.eid}] - 广告处理失败 - {traceback.format_exc()}")
            await self.redis_client.lpush(f"{self.redis_key}", video.eid)
        except UploadError:
            status = Status.UploadFailed
            self.logger.error(f"[{video.id}][{video.eid}] - 上传失败 重新推入队列 - {traceback.format_exc()}")
            # 必须是当前机器
            await self.redis_client.lpush(f"{self.redis_key}", video.eid)
        except NoSpaceError:
            status = Status.UploadFailed
            self.logger.error(f"[{video.id}][{video.eid}] - 下载失败，磁盘空间不足 - {traceback.format_exc()}")
        except EmptyM3U8Error as e:
            await self.handle(video.id, video.cid)
        except ForbiddenError as e:
            status = Status.Forbidden
            self.logger.error(f"[{video.id}][{video.eid}] - 403 Forbidden")
        except Exception as e:
            status = Status.UploadFailed
            self.logger.error(f"[{video.id}][{video.eid}] - 上传失败 未知错误 - {traceback.format_exc()}")
        finally:
            if status in (Status.DownloadFailed, Status.Success):
                self.clean(video, root.absolute(), status)
            return status, object_prefix

    async def handle(self, vid, cid):
        """删除现有的视频, 然后重新拉取海外看的播放链接，重新生成视频"""
        with scope_session() as session:
            rows: Optional[List[Type[V4Mappings]]] = session.query(V4Mappings).filter(V4Mappings.cid == cid, V4Mappings.tid.like('hw%')).all()
            mappings = copy.deepcopy(rows)
            for mapping in mappings:
                data = json.dumps({'vid': vid})
                await self.redis_client.lpush('clean_v4_videos', data)
                self.logger.warning(f"lpush clean_v4_videos {data}")
                await self.redis_client.lpush('tC4OncNGDDSSS_detail', json.dumps({"tid": mapping.tid}))
                self.logger.warning(f"lpush tC4OncNGDDSSS_detail {mapping.tid}")

    def clean(self, video, root, status):
        if video.status == Status.Torrent:
            return
        assert root.name != '/'
        if root.exists() and root.is_dir():
            shutil.rmtree(root, ignore_errors=True)
        time.sleep(0.5)
        if not root.exists():
            self.logger.info(f"[{video.id}][{status}] 删除目录下的文件成功 {root.absolute()}")
        else:
            self.logger.info(f"[{video.id}][{status}] 删除目录下的文件失败 {root.absolute()}")

    async def get_next_request(self):
        """监听队列"""
        try:
            task = await self.redis_client.blpop([f"{self.redis_key}", self.redis_key], timeout=3)
            return silent(int)(task[-1]) if task else None
        except TimeoutError:
            return None
        except aioredis.exceptions.ConnectionError as e:
            self.redis_client = get_aioredis_client(self.settings['REDIS_URI'])
            return None