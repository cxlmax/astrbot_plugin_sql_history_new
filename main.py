from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
from astrbot import logger
import aiomysql
import asyncio
import json
from datetime import datetime
from typing import Optional


@register("mysql_logger", "LW", "MySQL消息日志插件", "1.0.0")
class MySQLPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None
        self._init_lock = asyncio.Lock()  # 初始化锁，防止并发初始化
        self._init_attempted = False  # 是否尝试过初始化
        self._init_success = False  # 初始化是否成功

    async def _ensure_pool_initialized(self):
        """懒加载：确保连接池已初始化"""
        # 快速检查：如果已经初始化成功，直接返回
        if self._init_success and self.pool is not None:
            return

        # 使用锁防止并发初始化
        async with self._init_lock:
            # 双重检查：可能其他协程已经完成初始化
            if self._init_success and self.pool is not None:
                return

            # 如果之前初始化失败过，不再重试
            if self._init_attempted and not self._init_success:
                raise RuntimeError("数据库连接池初始化失败，插件已停止工作")

            self._init_attempted = True

            try:
                logger.info("[MySQL日志插件] 正在初始化数据库连接池...")

                # 创建连接池
                self.pool = await aiomysql.create_pool(
                    host=self.config.get("host"),
                    port=self.config.get("port"),
                    user=self.config.get("username"),
                    password=self.config.get("password"),
                    db=self.config.get("database"),
                    autocommit=True,
                    minsize=1,
                    maxsize=5,
                    connect_timeout=10
                )

                # 测试连接
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        result = await cursor.fetchone()
                        if result[0] != 1:
                            raise ConnectionError("数据库测试查询失败")

                # 创建表结构
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("""

                        CREATE TABLE IF NOT EXISTS `messages`  (
                            `id` bigint NOT NULL AUTO_INCREMENT,
                            `message_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                            `platform_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                            `self_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                            `session_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                            `group_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
                            `sender` json NOT NULL,
                            `message_str` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                            `raw_message` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
                            `timestamp` timestamp NOT NULL,
                            PRIMARY KEY (`id` DESC) USING BTREE)
                        """)

                self._init_success = True
                logger.info("[MySQL日志插件] 数据库连接池初始化成功")

            except Exception as e:
                logger.error(f"[MySQL日志插件] 数据库连接池初始化失败: {str(e)}")
                logger.error(f"[MySQL日志插件] 配置信息: host={self.config.get('host')}, "
                           f"port={self.config.get('port')}, "
                           f"database={self.config.get('database')}, "
                           f"username={self.config.get('username')}")
                # 清理失败的连接池
                if self.pool:
                    self.pool.close()
                    await self.pool.wait_closed()
                    self.pool = None
                raise

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        """处理所有消息事件"""
        try:
            # 懒加载：第一次调用时初始化数据库连接池
            await self._ensure_pool_initialized()

            # 从事件对象中获取消息信息
            msg = event.message_obj  # 消息对象
            meta = event.platform_meta  # 平台元数据

            # 序列化发送者信息
            sender_data = {
                'user_id': msg.sender.user_id if msg.sender else None,
                'nickname': msg.sender.nickname if msg.sender else None,
                'platform_id': meta.id
            }

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    timestamp_value = msg.timestamp
                    formatted_timestamp = None
                    if timestamp_value is not None:
                        try:
                            timestamp_float = float(timestamp_value)
                            if timestamp_float > 1_000_000_000_000:
                                timestamp_float /= 1000.0
                            formatted_timestamp = datetime.fromtimestamp(timestamp_float).strftime("%Y-%m-%d %H:%M:%S")
                        except (TypeError, ValueError):
                            logger.warning(f"[MySQL日志插件] 无法解析消息时间戳: {timestamp_value}")
                    if formatted_timestamp is None:
                        formatted_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # 使用 INSERT IGNORE 避免主键冲突
                    await cursor.execute("""
                        INSERT IGNORE INTO messages (
                            message_id,
                            platform_type,
                            self_id,
                            session_id,
                            group_id,
                            sender,
                            message_str,
                            raw_message,
                            timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        msg.message_id,
                        meta.name,
                        event.get_self_id(),
                        event.session_id,
                        msg.group_id if msg.group_id else None,
                        json.dumps(sender_data, ensure_ascii=False),
                        event.message_str,
                        json.dumps(msg.raw_message, ensure_ascii=False) if msg.raw_message else None,
                        formatted_timestamp
                    ))

            logger.debug(f"[MySQL日志插件] 已记录消息: {msg.message_id}")

        except RuntimeError as e:
            # 初始化失败，记录但不抛出异常，避免影响其他插件
            logger.error(f"[MySQL日志插件] {str(e)}")
        except Exception as e:
            logger.error(f"[MySQL日志插件] 记录消息失败: {str(e)}")
            logger.error(f"[MySQL日志插件] 消息ID: {msg.message_id if msg else 'unknown'}")

    async def terminate(self):
        """清理资源"""
        try:
            if self.pool:
                logger.info("[MySQL日志插件] 正在关闭数据库连接池...")
                self.pool.close()
                await self.pool.wait_closed()
                self.pool = None
                logger.info("[MySQL日志插件] 数据库连接池已关闭")
        except Exception as e:
            logger.error(f"[MySQL日志插件] 关闭连接池时出错: {str(e)}")
