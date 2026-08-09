#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import threading
import logging
import re
import signal
import gzip
import typing
import json
import tempfile
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict
import base64

# MQTT
import paho.mqtt.client as mqtt

# 配置日志
# 1. 创建 handlers
console_handler = logging.StreamHandler(sys.stdout)      # 输出到 stdout
console_handler.setLevel(logging.INFO)                   # 控制台只显示 INFO 及以上

file_handler = logging.FileHandler('ros_tsfile_bridge.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)                    # 文件记录 DEBUG 及以上

# 2. 配置 root logger
logging.basicConfig(
    level=logging.DEBUG,                                 # root 最低级别（影响所有未单独设级别的 handler）
    format='[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[console_handler, file_handler]
)

# 3. 获取子 logger（可额外设置其独立级别）
logger = logging.getLogger('Ros_to_TsFile')
# logger.setLevel(logging.DEBUG)   # 若需与 root 不同，可取消注释


# 检查 ROS 环境
ROS_VERSION = int(os.environ.get('ROS_VERSION', 0))
if ROS_VERSION != 1:
    sys.exit("Error: This version only supports ROS1. Please set ROS_VERSION=1")

import rospy
import roslib.message

# 导入 TsFile SDK
try:
    from tsfile import (
        TsFileTableWriter, TableSchema, ColumnSchema,
        Tablet, TSDataType, ColumnCategory
    )
except ImportError as e:
    logger.error(f"Failed to import tsfile: {e}")
    logger.error("Please install tsfile-python-sdk and ensure libtsfile.so is in LD_LIBRARY_PATH")
    sys.exit(1)

# 导入 rosbag 支持（可选）
try:
    import rosbag
except ImportError:
    rosbag = None
    logger.warning("rosbag module not found, bag playback mode will be unavailable")

# 导入传感器消息类型
from sensor_msgs.msg import Image, PointCloud2


class TopicManager:
    """
    管理单个 ROS 主题的 TsFile 写入。
    支持两种存储模式：
    - embedded: 图像/点云数据（可选压缩后）直接作为 STRING 列存入 TsFile（底层字节流）
    - external: 图像/点云数据（可选压缩）追加到外部文件，TsFile 中仅存储索引
    """

    def __init__(self, topic_name: str, schema: TableSchema, base_path: str,
                 buffer_size: int = 100, max_file_size_mb: int = 100,
                 use_msg_stamp: bool = True, flush_interval: float = 0.5,
                 storage_mode: str = "embedded", compress_blob: bool = False):
        """
        Args:
            topic_name: ROS 主题名
            schema: TsFile 表结构
            base_path: 数据根目录
            buffer_size: 内存缓冲行数（达到此值即 flush）
            max_file_size_mb: 单个 TsFile 文件最大大小（MB）
            use_msg_stamp: 是否使用 ROS 消息自带的时间戳
            flush_interval: 定时刷新间隔（秒）
            storage_mode: 存储模式，"embedded" 或 "external"
            compress_blob: 是否对二进制数据进行 gzip 压缩
                           - embedded: 压缩后直接以 STRING 存储
                           - external: 压缩后追加到外部文件
        """
        self.clean_topic = re.sub(r'[^a-zA-Z0-9_]', '_', topic_name.strip('/'))
        self.schema = schema
        self.base_dir = os.path.join(base_path, self.clean_topic)
        os.makedirs(self.base_dir, exist_ok=True)

        # 保存 schema 到文件
        self._save_schema()

        self.buffer_size = buffer_size
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        # 轮转预判阈值：用逻辑字节预估磁盘写入，需留安全余量
        # （TsFile 序列化开销 + deflate 对不可压缩数据的膨胀约 3~5%），保证实际文件不超 max_file_size_mb
        self._rotate_pre_threshold = int(self.max_file_size_bytes * 0.90)
        self.use_msg_stamp = use_msg_stamp
        self.flush_interval = flush_interval
        self.storage_mode = storage_mode
        self.compress_blob = compress_blob

        self.writer: Optional[TsFileTableWriter] = None
        self.current_tsfile_path: Optional[str] = None
        self.row_count = 0
        self._pending_bytes = 0  # 当前 tablet 中尚未落盘的估算逻辑字节（用于 max_file_size_mb 轮转预判）
        self.lock = threading.RLock()
        self.last_timestamp_ms = 0

        # 外部存储模式专用资源
        self.data_dir = None
        self.current_data_file: Optional[typing.BinaryIO] = None
        self.current_data_file_path: Optional[str] = None
        self.current_data_file_size: int = 0

        if self.storage_mode == "external":
            self.data_dir = os.path.join(self.base_dir, "data")
            os.makedirs(self.data_dir, exist_ok=True)
            self._open_data_file()

        # 定时刷新控制
        self._stop_timer = False
        self._timer = None

        # 预提取列信息
        self.columns = list(schema.get_columns())
        self.col_names = [c.column_name for c in self.columns]
        self.col_types = [c.data_type for c in self.columns]
        self.col_index = {name: idx for idx, name in enumerate(self.col_names)}

        self._init_tsfile_writer()
        self._start_flush_timer()

    def _save_schema(self):
        """将 TableSchema 保存为 JSON 文件到主题目录"""
        schema_dict = {
            "table_name": self.schema.table_name,
            "columns": []
        }
        for col in self.schema.get_columns():
            schema_dict["columns"].append({
                "name": col.column_name,
                "data_type": col.data_type.name,
                "category": col.category.name
            })
        schema_path = os.path.join(self.base_dir, "schema.json")
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir=self.base_dir, delete=False) as tf:
                json.dump(schema_dict, tf, indent=2)
                temp_path = tf.name
            os.replace(temp_path, schema_path)
            logger.debug(f"Saved schema to {schema_path}")
        except Exception as e:
            logger.error(f"Failed to save schema to {schema_path}: {e}")

    def _init_tsfile_writer(self):
        """创建新的 TsFile 写入器"""
        with self.lock:
            try:
                if self.writer:
                    self.writer.close()
                ts_path = os.path.join(self.base_dir, f"{time.strftime('%Y%m%d_%H%M%S')}_{time.time_ns()}.tsfile")
                self.writer = TsFileTableWriter(ts_path, self.schema)
                self.current_tsfile_path = ts_path
                self.last_timestamp_ms = 0
                self.tablet = Tablet(self.col_names, self.col_types, self.buffer_size)
                self.row_count = 0
                self._pending_bytes = 0
                logger.info(f"New TsFile writer created: {ts_path}, buffer_size={self.buffer_size}")
            except Exception as e:
                logger.error(f"Failed to create writer for {self.clean_topic}: {e}", exc_info=True)
                self.writer = None
                self.current_tsfile_path = None

    # ---------- 外部存储专用方法 ----------
    def _open_data_file(self):
        """创建新的数据文件（文件名格式：YYYYMMDD_HHMMSS_纳秒.bin），并打开为写入模式"""
        with self.lock:
            if self.current_data_file:
                self.current_data_file.close()
            timestamp_str = time.strftime("%Y%m%d_%H%M%S")
            nanos = time.time_ns()
            filename = f"{timestamp_str}_{nanos}.bin"
            self.current_data_file_path = os.path.join(self.data_dir, filename)
            self.current_data_file = open(self.current_data_file_path, "wb")
            self.current_data_file_size = 0
            logger.debug(f"Created new data file: {self.current_data_file_path}")

    def _rotate_data_file_if_needed(self, additional_bytes: int) -> bool:
        """检查当前数据文件加上 additional_bytes 后是否超过限制，若超过则轮转"""
        with self.lock:
            if self.current_data_file_size + additional_bytes > self.max_file_size_bytes:
                logger.info(f"Data file {self.current_data_file_path} size {self.current_data_file_size} + {additional_bytes} exceeds limit, rotating")
                self._open_data_file()
                return True
            return False

    def _append_blob_data(self, raw_data: bytes) -> Tuple[str, int, int, int]:
        """
        根据 compress_blob 标志，将原始数据（可选压缩）追加到当前数据文件末尾。
        返回: (file_path, offset, stored_size, original_size)
        - 若 compress_blob=True: stored_size 为压缩后大小，original_size 为原始大小
        - 若 compress_blob=False: stored_size == original_size
        """
        if self.compress_blob:
            data = gzip.compress(raw_data, compresslevel=6)
            stored_size = len(data)
            original_size = len(raw_data)
        else:
            data = raw_data
            stored_size = len(data)
            original_size = len(data)

        with self.lock:
            self._rotate_data_file_if_needed(stored_size)
            offset = self.current_data_file_size
            self.current_data_file.write(data)
            self.current_data_file.flush()
            self.current_data_file_size += stored_size
            rel_path = os.path.join("data", os.path.basename(self.current_data_file_path))
            return rel_path, offset, stored_size, original_size

    def write_blob_index(self, timestamp_ms: int, extra_metadata: Dict[str, Any], blob_data: bytes) -> None:
        """
        外部存储模式：将二进制数据（可选压缩）追加到数据文件，并在 TsFile 中写入索引记录。
        :param timestamp_ms: 时间戳（毫秒），用作 TsFile 记录的时间戳（不存储为独立字段）
        :param extra_metadata: 需要存入 TsFile 的额外字段（如图像的编码、宽高等）
        :param blob_data: 原始二进制数据
        """
        if self.storage_mode != "external":
            raise RuntimeError(f"write_blob_index called in non-external mode for {self.clean_topic}")

        file_path, offset, stored_size, original_size = self._append_blob_data(blob_data)
        row_dict = {
            "file_path": file_path,
            "offset": offset,
            "compressed_size": stored_size,
            "uncompressed_size": original_size,
            **extra_metadata
        }
        self.write(row_dict, msg_stamp_ms=timestamp_ms, raw_bin=None)

    # ---------- 通用写入方法 ----------
    def write(self, data_dict: Dict[str, Any], msg_stamp_ms: Optional[int] = None,
              raw_bin: Optional[Dict[str, bytes]] = None):
        """写入一条记录到 TsFile（适用于所有模式）"""
        with self.lock:
            if not self.writer:
                self._init_tsfile_writer()
                if not self.writer:
                    logger.warning("Writer unavailable, dropping message")
                    return

            if msg_stamp_ms is not None and self.use_msg_stamp:
                timestamp_ms = msg_stamp_ms
            else:
                timestamp_ms = int(time.time() * 1000)

            if timestamp_ms <= self.last_timestamp_ms:
                timestamp_ms = self.last_timestamp_ms + 1
                logger.debug(f"Adjusted timestamp: {self.last_timestamp_ms} -> {timestamp_ms}")
            self.last_timestamp_ms = timestamp_ms

            # ---- max_file_size_mb 轮转预判 ----
            # 若当前 TsFile 磁盘大小 + tablet 待写字节 + 本行字节 将超过限制，
            # 先把已缓冲数据落盘并轮转到新文件，再让本行进入新文件。
            # 用逻辑字节（未压缩）预判是保守估计：可压缩数据会略小于限制，但绝不超限。
            row_bytes = 0
            for col_name in self.col_names:
                v = data_dict.get(col_name)
                if isinstance(v, str):
                    row_bytes += len(v)
                elif isinstance(v, (bytes, bytearray)):
                    row_bytes += len(v)
                elif v is not None:
                    row_bytes += 8  # 数值/布尔列固定开销
            row_bytes += 8 * len(self.col_names)  # 时间戳等固定开销

            if row_bytes > 0 and self.row_count > 0:
                cur_disk = os.path.getsize(self.current_tsfile_path) if self.current_tsfile_path else 0
                if cur_disk + self._pending_bytes + row_bytes > self._rotate_pre_threshold:
                    logger.info(
                        f"TsFile {os.path.basename(self.current_tsfile_path)} would exceed "
                        f"{self.max_file_size_bytes / 1048576:.0f}MB (disk {cur_disk / 1048576:.1f}MB + "
                        f"pending {self._pending_bytes / 1048576:.1f}MB), flushing {self.row_count} rows "
                        f"and rotating"
                    )
                    self._flush_tsfile()
                    self._init_tsfile_writer()

            try:
                self.tablet.add_timestamp(self.row_count, timestamp_ms)
                for col_name in self.col_names:
                    val = data_dict.get(col_name)
                    if val is None:
                        col_type = self.col_types[self.col_index[col_name]]
                        if col_type == TSDataType.STRING:
                            val = ""
                        elif col_type == TSDataType.INT64:
                            val = 0
                        elif col_type == TSDataType.DOUBLE:
                            val = 0.0
                        else:
                            val = 0
                    elif isinstance(val, bool):
                        val = 1 if val else 0
                    elif isinstance(val, int):
                        if val < -2**63 or val >= 2**63:
                            val = str(val)
                    # 对于 STRING 类型，如果 val 是 bytes，直接保留（不进行解码）
                    # TsFile 的 STRING 底层可存储任意字节流
                    if self.col_types[self.col_index[col_name]] == TSDataType.STRING and isinstance(val, bytes):
                        # 直接传递 bytes，Tablet 应能处理
                        pass
                    elif isinstance(val, bytes):
                        # 非 STRING 列出现 bytes，转换为字符串
                        val = val.decode('utf-8', errors='ignore')
                    self.tablet.add_value_by_name(col_name, self.row_count, val)
                self.row_count += 1
                self._pending_bytes += row_bytes
            except Exception as e:
                logger.error(f"Tablet fill error: {e}", exc_info=True)
                return

            if self.row_count >= self.buffer_size:
                self._flush_tsfile()

    def _flush_tsfile(self):
        """强制将当前 tablet 写入 TsFile 文件"""
        if self.row_count == 0 or not self.writer:
            return
        try:
            self.writer.write_table(self.tablet)
            self.writer.flush()
            logger.debug(f"Flushed {self.row_count} rows to {self.current_tsfile_path}")
            if os.path.getsize(self.current_tsfile_path) > self.max_file_size_bytes:
                logger.info(f"TsFile size exceeded, rotating TsFile writer")
                self._init_tsfile_writer()
        except Exception as e:
            logger.error(f"write_table failed: {e}", exc_info=True)
            try:
                self.writer.write_table(self.tablet)
                logger.info("Retry succeeded")
            except Exception as e2:
                logger.error(f"Retry also failed, data lost: {e2}")
        finally:
            self.tablet = Tablet(self.col_names, self.col_types, self.buffer_size)
            self.row_count = 0
            self._pending_bytes = 0

    def _start_flush_timer(self):
        def periodic_flush():
            if self._stop_timer:
                return
            with self.lock:
                if self.row_count > 0:
                    self._flush_tsfile()
            if not self._stop_timer:
                self._timer = threading.Timer(self.flush_interval, periodic_flush)
                self._timer.daemon = True
                self._timer.start()

        self._timer = threading.Timer(self.flush_interval, periodic_flush)
        self._timer.daemon = True
        self._timer.start()

    def shutdown(self):
        self._stop_timer = True
        if self._timer:
            self._timer.cancel()
        with self.lock:
            if self.row_count > 0:
                logger.info(f"Shutdown: flushing remaining {self.row_count} rows")
                self._flush_tsfile()
            if self.writer:
                try:
                    self.writer.close()
                    logger.info(f"Closed TsFile: {self.current_tsfile_path}")
                except Exception as e:
                    logger.error(f"Error closing writer: {e}")
                self.writer = None
            if self.storage_mode == "external" and self.current_data_file:
                try:
                    self.current_data_file.close()
                    logger.info(f"Closed data file: {self.current_data_file_path}")
                except Exception as e:
                    logger.error(f"Error closing data file: {e}")
                self.current_data_file = None


class MqttForwarder:
    """ROS → MQTT 消息转发器，将 ROS 消息元数据以 JSON 格式发布到对应 MQTT 主题"""

    def __init__(self, host: str, port: int = 1883, root_topic: str = "ros", qos: int = 0):
        self.host = host
        self.port = port
        self.root_topic = root_topic.strip('/') if root_topic else ""
        self.qos = qos
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._lock = threading.Lock()
        self._started = False

    def start(self):
        """连接 MQTT broker 并启动网络线程"""
        try:
            self.client.connect_async(self.host, self.port, keepalive=60)
            self.client.loop_start()
            self._started = True
            print(f"[Ros_to_TsFile] MQTT forwarder connected to {self.host}:{self.port}, root_topic={self.root_topic or '/'}", flush=True)
            logger.info(f"MQTT forwarder connected to {self.host}:{self.port}, root_topic={self.root_topic or '/'}")
        except Exception as e:
            logger.error(f"Failed to connect MQTT broker: {e}")
            self._started = False

    def _ros_topic_to_mqtt(self, ros_topic: str) -> str:
        """将 ROS 主题名映射为 MQTT 主题名。
        例如: root_topic='ros', ros_topic='/camera/image_raw' → 'ros/camera/image_raw'
        """
        clean = ros_topic.strip('/')
        return f"{self.root_topic}/{clean}" if self.root_topic else clean

    def forward(self, ros_topic: str, payload_dict: Dict[str, Any]):
        """将 payload 字典以 JSON 形式发布到对应 MQTT 主题"""
        if not self._started:
            return
        mqtt_topic = self._ros_topic_to_mqtt(ros_topic)
        try:
            payload_json = json.dumps(payload_dict)
            with self._lock:
                self.client.publish(mqtt_topic, payload_json, qos=self.qos)
            logger.debug(f"MQTT: {ros_topic} → {mqtt_topic}")
        except Exception as e:
            logger.error(f"Failed to forward MQTT message for {ros_topic}: {e}")

    def shutdown(self):
        """断开 MQTT 连接"""
        if self._started:
            self.client.loop_stop()
            self.client.disconnect()
            self._started = False
            logger.info("MQTT forwarder shut down")


class MqttCommander:
    """MQTT 命令控制器：订阅 /cmd/{bridge_id}/# 接收远程命令"""

    def __init__(self, bridge_id: str, bridge: 'RosToTsFileBridge',
                 mqtt_host: str, mqtt_port: int = 1883):
        self.bridge_id = bridge_id
        self.bridge = bridge
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_message = self._on_command
        self._cmd_root = f"/cmd/{bridge_id}"

        self.client.connect_async(mqtt_host, mqtt_port, keepalive=60)
        self.client.loop_start()
        # 延迟订阅 (connect_async 非阻塞)
        time.sleep(0.3)
        try:
            self.client.subscribe(f"{self._cmd_root}/#", qos=1)
            print(f"[Ros_to_TsFile] MQTT Commander ready: {self._cmd_root}/#", flush=True)
            logger.info(f"MQTT Commander subscribed to {self._cmd_root}/#")
        except Exception as e:
            logger.error(f"MQTT Commander subscribe failed: {e}")
            self.client = None

    def _publish_rsp(self, action: str, ok: bool, message: str, extra: dict = None):
        """发布命令执行响应到 /cmd_rsp/{bridge_id}/{action}"""
        rsp_topic = f"/cmd_rsp/{self.bridge_id}/{action}"
        rsp = {"ok": ok, "message": message}
        if extra:
            rsp.update(extra)
        try:
            self.client.publish(rsp_topic, json.dumps(rsp), qos=1)
            status = "OK" if ok else "FAIL"
            logger.info(f"CMD_RSP [{status}] {rsp_topic}: {rsp}")
        except Exception as e:
            logger.error(f"Failed to publish CMD_RSP: {e}")

    def _on_command(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8")) if msg.payload else {}
        except json.JSONDecodeError:
            payload = {}

        logger.info(f"CMD received: {topic} payload={payload}")

        if topic.endswith("/start"):
            self._cmd_start(payload)
        elif topic.endswith("/stop"):
            self._cmd_stop(payload)
        elif topic.endswith("/delete"):
            self._cmd_delete(payload)
        elif topic.endswith("/startAll"):
            self._cmd_start_all()
        elif topic.endswith("/stopAll"):
            self._cmd_stop_all()
        elif topic.endswith("/save"):
            self._cmd_save()
        else:
            logger.warning(f"Unknown command topic: {topic}")

    def _cmd_start(self, payload: dict):
        topic = payload.get("topic", "")
        if not topic:
            print("[CMD] start: missing topic", flush=True)
            self._publish_rsp("start", False, "missing topic field")
            return
        print(f"[CMD] start topic: {topic}", flush=True)
        ok, msg = self.bridge.add_interested_topic(topic)
        self._publish_rsp("start", ok, msg, {"topic": topic})

    def _cmd_stop(self, payload: dict):
        topic = payload.get("topic", "")
        if not topic:
            print("[CMD] stop: missing topic", flush=True)
            self._publish_rsp("stop", False, "missing topic field")
            return
        print(f"[CMD] stop topic: {topic}", flush=True)
        ok, msg = self.bridge.remove_interested_topic(topic)
        self._publish_rsp("stop", ok, msg, {"topic": topic})

    def _cmd_delete(self, payload: dict):
        topic = payload.get("topic", "")
        if not topic:
            print("[CMD] delete: missing topic", flush=True)
            self._publish_rsp("delete", False, "missing topic field")
            return
        print(f"[CMD] delete topic: {topic}", flush=True)
        # 如果正在采集则先停止
        self.bridge.remove_interested_topic(topic)
        # 从持久化文件移除
        ok, msg = self.bridge.delete_topic_from_file(topic)
        self._publish_rsp("delete", ok, msg, {"topic": topic})

    def _cmd_start_all(self):
        print("[CMD] startAll — subscribing to all topics", flush=True)
        ok, msg = self.bridge.start_all_topics()
        self._publish_rsp("startAll", ok, msg)

    def _cmd_stop_all(self):
        print("[CMD] stopAll — flushing and stopping all topics", flush=True)
        ok, msg = self.bridge.stop_all_topics()
        self._publish_rsp("stopAll", ok, msg)

    def _cmd_save(self):
        print("[CMD] save — saving subscribed topics to sub_topics.txt", flush=True)
        ok, msg, count = self.bridge.save_topics_to_file()
        self._publish_rsp("save", ok, msg, {"topics_count": count})

    def shutdown(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT Commander shut down")


class RosToTsFileBridge:
    """ROS1 到 TsFile 桥接主类，支持 embedded / external 存储 + MQTT 远程命令"""

    def __init__(self, root_path: str = "ros_data_assets",
                 default_buffer_size: int = 100,
                 max_file_size_mb: int = 100,
                 interested_topics: Optional[List[str]] = None,
                 use_msg_stamp: bool = True,
                 flush_interval: float = 0.5,
                 stats_interval: float = 10.0,
                 bag_progress_interval: int = 10000,
                 storage_mode: str = "embedded",
                 compress_blob: bool = False,
                 discovery_interval: float = 5.0,
                 mqtt_host: Optional[str] = None,
                 mqtt_port: int = 1883,
                 mqtt_root_topic: str = "ros",
                 bridge_id: Optional[str] = None):
        self.root_path = os.path.abspath(root_path)
        self.default_buffer_size = default_buffer_size
        self.max_file_size_mb = max_file_size_mb
        self.use_msg_stamp = use_msg_stamp
        self.flush_interval = flush_interval
        self.stats_interval = stats_interval
        self.bag_progress_interval = bag_progress_interval
        self.storage_mode = storage_mode
        self.compress_blob = compress_blob
        self.discovery_interval = discovery_interval

        # Bridge ID
        self.bridge_id = bridge_id or f"bridge_{os.uname().nodename}_{os.getpid()}"
        self._topics_file = os.path.join(self.root_path, "sub_topics.txt")

        # 感兴趣主题 (可变集合，支持运行时增删)
        self._interested_lock = threading.RLock()
        if interested_topics:
            self._interested_set: Optional[set] = set(interested_topics)
        else:
            # 尝试从文件加载
            loaded = self._load_topics_from_file()
            if loaded:
                self._interested_set = loaded
            else:
                self._interested_set = None  # None = 全部主题

        self.managers: Dict[str, TopicManager] = {}

        self.msg_count = 0
        self.topic_counts: Dict[str, int] = defaultdict(int)
        self.last_stats_time = time.time()
        self._stats_thread: Optional[threading.Thread] = None
        self._stop_stats = False

        # MQTT 转发
        self.mqtt_forwarder: Optional[MqttForwarder] = None
        if mqtt_host is not None:
            self.mqtt_forwarder = MqttForwarder(
                host=mqtt_host, port=mqtt_port,
                root_topic=mqtt_root_topic
            )

        # MQTT 命令控制 (bridge_id + mqtt_host 都存在才启用)
        self.mqtt_commander: Optional[MqttCommander] = None
        if mqtt_host is not None:
            self.mqtt_commander = MqttCommander(
                bridge_id=self.bridge_id, bridge=self,
                mqtt_host=mqtt_host, mqtt_port=mqtt_port
            )

        # 动态主题发现相关
        self._subscribed_topics: set = set()
        self._failed_subscribe_topics: set = set()
        self._ros_subscribers: Dict[str, object] = {}  # rospy.Subscriber 句柄
        self._discovery_thread: Optional[threading.Thread] = None
        self._stop_discovery = False

    def _extract_stamp(self, msg) -> Optional[int]:
        if hasattr(msg, 'header') and hasattr(msg.header, 'stamp'):
            stamp = msg.header.stamp
            return int(stamp.secs * 1000 + stamp.nsecs / 1e6)
        if hasattr(msg, 'stamp'):
            stamp = msg.stamp
            if isinstance(stamp, rospy.Time):
                return int(stamp.secs * 1000 + stamp.nsecs / 1e6)
        return None

    def _extract_header_info(self, msg) -> Dict[str, Any]:
        header_info = {
            "header_seq": 0,
            "header_stamp_secs": 0,
            "header_stamp_nsecs": 0,
            "header_frame_id": "",
        }
        if hasattr(msg, 'header'):
            header = msg.header
            if hasattr(header, 'seq'):
                header_info["header_seq"] = header.seq
            if hasattr(header, 'stamp'):
                header_info["header_stamp_secs"] = header.stamp.secs
                header_info["header_stamp_nsecs"] = header.stamp.nsecs
            if hasattr(header, 'frame_id'):
                header_info["header_frame_id"] = header.frame_id
        return header_info

    def _get_image_schema(self, topic: str) -> TableSchema:
        """根据存储模式返回图像表结构（embedded 模式 data 列为 STRING 类型存储原始字节流）"""
        common_columns = [
            ColumnSchema("header_seq", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("header_stamp_secs", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("header_stamp_nsecs", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("header_frame_id", TSDataType.STRING, ColumnCategory.FIELD),
            ColumnSchema("encoding", TSDataType.STRING, ColumnCategory.FIELD),
            ColumnSchema("width", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("height", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("step", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("is_bigendian", TSDataType.INT32, ColumnCategory.FIELD),
        ]
        if self.storage_mode == "embedded":
            columns = [ColumnSchema("data", TSDataType.STRING, ColumnCategory.FIELD)] + common_columns
        else:  # external
            columns = [
                ColumnSchema("file_path", TSDataType.STRING, ColumnCategory.FIELD),
                ColumnSchema("offset", TSDataType.INT64, ColumnCategory.FIELD),
                ColumnSchema("compressed_size", TSDataType.INT64, ColumnCategory.FIELD),
                ColumnSchema("uncompressed_size", TSDataType.INT64, ColumnCategory.FIELD),
            ] + common_columns
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', topic.strip('/'))
        return TableSchema(table_name, columns)

    def _get_pointcloud_schema(self, topic: str) -> TableSchema:
        """根据存储模式返回点云表结构（embedded 模式 data 列为 STRING 类型存储原始字节流）"""
        common_columns = [
            ColumnSchema("header_seq", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("header_stamp_secs", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("header_stamp_nsecs", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("header_frame_id", TSDataType.STRING, ColumnCategory.FIELD),
            ColumnSchema("num_points", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("fields", TSDataType.STRING, ColumnCategory.FIELD),
            ColumnSchema("is_bigendian", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("point_step", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("row_step", TSDataType.INT32, ColumnCategory.FIELD),
            ColumnSchema("is_dense", TSDataType.INT32, ColumnCategory.FIELD),
        ]
        if self.storage_mode == "embedded":
            columns = [ColumnSchema("data", TSDataType.STRING, ColumnCategory.FIELD)] + common_columns
        else:
            columns = [
                ColumnSchema("file_path", TSDataType.STRING, ColumnCategory.FIELD),
                ColumnSchema("offset", TSDataType.INT64, ColumnCategory.FIELD),
                ColumnSchema("compressed_size", TSDataType.INT64, ColumnCategory.FIELD),
                ColumnSchema("uncompressed_size", TSDataType.INT64, ColumnCategory.FIELD),
            ] + common_columns
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', topic.strip('/'))
        return TableSchema(table_name, columns)

    def _get_generic_schema(self, flat_data: Dict[str, Any], topic: str) -> TableSchema:
        columns = []
        for col_name, val in flat_data.items():
            if val == "EXTERNAL_BIN":
                dtype = TSDataType.STRING
            elif isinstance(val, bool):
                dtype = TSDataType.INT32
            elif isinstance(val, int):
                dtype = TSDataType.INT64
            elif isinstance(val, float):
                dtype = TSDataType.DOUBLE
            else:
                dtype = TSDataType.STRING
            columns.append(ColumnSchema(col_name, dtype, ColumnCategory.FIELD))
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', topic.strip('/'))
        return TableSchema(table_name, columns)

    def _process_image_message(self, topic: str, msg: Image, msg_type: str):
        try:
            stamp_ms = self._extract_stamp(msg) if self.use_msg_stamp else None
            if stamp_ms is None:
                stamp_ms = 0

            header_info = self._extract_header_info(msg)

            if topic not in self.managers:
                if not self._is_topic_interested(self._normalize_topic(topic)):
                    return  # 已 stop，跳过
                schema = self._get_image_schema(topic)
                self.managers[topic] = TopicManager(
                    topic_name=topic,
                    schema=schema,
                    base_path=self.root_path,
                    buffer_size=self.default_buffer_size,
                    max_file_size_mb=self.max_file_size_mb,
                    use_msg_stamp=self.use_msg_stamp,
                    flush_interval=self.flush_interval,
                    storage_mode=self.storage_mode,
                    compress_blob=self.compress_blob
                )
                logger.info(f"Created image manager for topic '{topic}' (mode={self.storage_mode}, compress={self.compress_blob})")

            extra_metadata = {
                "encoding": msg.encoding,
                "width": msg.width,
                "height": msg.height,
                "step": msg.step,
                "is_bigendian": 1 if msg.is_bigendian else 0,
                **header_info,
            }

            if self.storage_mode == "embedded":
                image_data = msg.data
                if self.compress_blob:
                    image_data = gzip.compress(image_data, compresslevel=6)
                # 直接存储 bytes 到 data 列（STRING 类型）
                data_str = base64.b64encode(image_data).decode('ascii')
                row_dict = {"data": data_str, **extra_metadata}
                self.managers[topic].write(row_dict, msg_stamp_ms=stamp_ms)
            else:  # external
                self.managers[topic].write_blob_index(
                    timestamp_ms=stamp_ms,
                    extra_metadata=extra_metadata,
                    blob_data=msg.data
                )

            # MQTT 转发：仅发送元数据，不发送原始像素数据
            self._forward_to_mqtt(topic, {"type": msg_type, **extra_metadata})

            self.msg_count += 1
            self.topic_counts[topic] += 1
        except Exception as e:
            logger.error(f"Failed to process image on {topic}: {e}", exc_info=True)

    def _process_pointcloud_message(self, topic: str, msg: PointCloud2, msg_type: str):
        try:
            stamp_ms = self._extract_stamp(msg) if self.use_msg_stamp else None
            if stamp_ms is None:
                stamp_ms = 0

            header_info = self._extract_header_info(msg)

            if topic not in self.managers:
                if not self._is_topic_interested(self._normalize_topic(topic)):
                    return  # 已 stop，跳过
                schema = self._get_pointcloud_schema(topic)
                self.managers[topic] = TopicManager(
                    topic_name=topic,
                    schema=schema,
                    base_path=self.root_path,
                    buffer_size=self.default_buffer_size,
                    max_file_size_mb=self.max_file_size_mb,
                    use_msg_stamp=self.use_msg_stamp,
                    flush_interval=self.flush_interval,
                    storage_mode=self.storage_mode,
                    compress_blob=self.compress_blob
                )
                logger.info(f"Created pointcloud manager for topic '{topic}' (mode={self.storage_mode}, compress={self.compress_blob})")

            fields_str = ";".join([f"{f.name}:{f.datatype}:{f.offset}" for f in msg.fields])
            extra_metadata = {
                "num_points": msg.width * msg.height,
                "fields": fields_str,
                "is_bigendian": 1 if msg.is_bigendian else 0,
                "point_step": msg.point_step,
                "row_step": msg.row_step,
                "is_dense": 1 if msg.is_dense else 0,
                **header_info,
            }

            if self.storage_mode == "embedded":
                pointcloud_data = msg.data
                if self.compress_blob:
                    pointcloud_data = gzip.compress(pointcloud_data, compresslevel=6)
                data_str = base64.b64encode(pointcloud_data).decode('ascii')
                row_dict = {"data": data_str, **extra_metadata}
                self.managers[topic].write(row_dict, msg_stamp_ms=stamp_ms)
            else:  # external
                self.managers[topic].write_blob_index(
                    timestamp_ms=stamp_ms,
                    extra_metadata=extra_metadata,
                    blob_data=msg.data
                )

            # MQTT 转发：仅发送元数据，不发送原始点云数据
            self._forward_to_mqtt(topic, {"type": msg_type, **extra_metadata})

            self.msg_count += 1
            self.topic_counts[topic] += 1
        except Exception as e:
            logger.error(f"Failed to process pointcloud on {topic}: {e}", exc_info=True)

    def _flatten_msg(self, msg, prefix: str = "") -> Dict[str, Any]:
        """递归展平 ROS 消息，避免递归错误，安全处理列表中的复杂对象"""
        result = {}
        if hasattr(msg, "__slots__"):
            for slot in msg.__slots__:
                val = getattr(msg, slot)
                key = f"{prefix}{slot}" if prefix else slot
                if isinstance(val, (bytes, bytearray)):
                    if len(val) > 512:
                        result[key] = "EXTERNAL_BIN"
                    else:
                        result[key] = val.decode('utf-8', errors='ignore')
                elif hasattr(val, "__slots__"):
                    result.update(self._flatten_msg(val, f"{key}_"))
                elif isinstance(val, (list, tuple)):
                    # 安全转换列表/元组，避免消息对象递归
                    max_display = 20
                    truncated = val[:max_display] if len(val) > max_display else val
                    def safe_repr(x):
                        # 如果是 ROS 消息，只显示类型名
                        if hasattr(x, "__slots__") and hasattr(x, "_type"):
                            return f"<{x._type}>"
                        return repr(x)
                    items = [safe_repr(v) for v in truncated]
                    if len(val) > max_display:
                        items.append("...")
                    result[key] = "[" + ", ".join(items) + "]"
                else:
                    result[key] = val
        else:
            result[prefix or "data"] = str(msg)
        return result

    def process_message(self, topic: str, msg, msg_type: str):
        if msg_type == 'sensor_msgs/Image':
            self._process_image_message(topic, msg, msg_type)
            return
        if msg_type == 'sensor_msgs/PointCloud2':
            self._process_pointcloud_message(topic, msg, msg_type)
            return

        try:
            self.msg_count += 1
            self.topic_counts[topic] += 1

            stamp_ms = self._extract_stamp(msg) if self.use_msg_stamp else None
            flat_data = self._flatten_msg(msg)

            if topic not in self.managers:
                if not self._is_topic_interested(self._normalize_topic(topic)):
                    return  # 已 stop，跳过
                schema = self._get_generic_schema(flat_data, topic)
                self.managers[topic] = TopicManager(
                    topic_name=topic,
                    schema=schema,
                    base_path=self.root_path,
                    buffer_size=self.default_buffer_size,
                    max_file_size_mb=self.max_file_size_mb,
                    use_msg_stamp=self.use_msg_stamp,
                    flush_interval=self.flush_interval,
                    storage_mode=self.storage_mode,
                    compress_blob=False  # 通用消息不压缩
                )
                logger.info(f"Created generic manager for topic '{topic}'")

            raw_bin = {}
            for k, v in flat_data.items():
                if v == "EXTERNAL_BIN" and hasattr(msg, k):
                    bin_data = getattr(msg, k)
                    if isinstance(bin_data, (bytes, bytearray)):
                        raw_bin[k] = bin_data
                        flat_data[k] = ""

            self.managers[topic].write(flat_data, msg_stamp_ms=stamp_ms, raw_bin=raw_bin if raw_bin else None)

            # MQTT 转发：发送展平数据（大二进制字段已替换为空字符串）
            self._forward_to_mqtt(topic, {"type": msg_type, **flat_data})

        except Exception as e:
            logger.error(f"Failed to process message on {topic}: {e}", exc_info=True)

    def _forward_to_mqtt(self, topic: str, payload: Dict[str, Any]):
        """将消息元数据转发到 MQTT（如已启用）"""
        if self.mqtt_forwarder:
            self.mqtt_forwarder.forward(topic, payload)

    # ---------- 统计与生命周期 ----------
    def _print_stats(self):
        now = time.time()
        elapsed = now - self.last_stats_time
        if elapsed < 0.001:
            return
        total_rate = self.msg_count / elapsed
        logger.info(f"=== Stats: {self.msg_count} msgs total, {total_rate:.1f} msg/s ===")
        for topic, cnt in list(self.topic_counts.items()):
            topic_rate = cnt / elapsed
            logger.info(f"  {topic}: {cnt} msgs ({topic_rate:.1f} msg/s)")
        self.msg_count = 0
        self.topic_counts.clear()
        self.last_stats_time = now

    def _stats_loop(self):
        while not self._stop_stats:
            time.sleep(self.stats_interval)
            if not self._stop_stats:
                self._print_stats()

    def _normalize_topic(self, topic: str) -> str:
        """统一主题名格式：去掉首尾 '/'，便于匹配"""
        return topic.strip('/') if topic else topic

    def _get_normalized_interested(self) -> Optional[set]:
        """返回规范化后的感兴趣主题集合 (None=全部)"""
        with self._interested_lock:
            if self._interested_set is None:
                return None
            return set(self._interested_set)  # 返回副本

    def _is_topic_interested(self, norm_topic: str) -> bool:
        """判断主题是否匹配用户感兴趣列表。支持：
        - 精确匹配：/a/b/c 匹配 /a/b/c
        - 命名空间前缀匹配：/a/b/ 匹配 /a/b/c、/a/b/c/d 等
        """
        normalized_interested = self._get_normalized_interested()
        if normalized_interested is None:
            return True  # None = 全部主题
        if not normalized_interested:
            return False  # 空集合 = 无感兴趣主题

        for interested in normalized_interested:
            if interested == norm_topic:
                return True
            # 如果感兴趣主题以 '/' 结尾，视为命名空间前缀
            if interested.endswith('/') and norm_topic.startswith(interested):
                return True
        return False

    # ────── 远程命令：动态管理感兴趣主题 ──────
    def add_interested_topic(self, topic: str) -> tuple:
        """添加一个主题到感兴趣列表，返回 (ok, message)"""
        norm = self._normalize_topic(topic)
        with self._interested_lock:
            if self._interested_set is None:
                msg = f"topic '{norm}' already covered (all-topics mode)"
                print(f"[CMD] {msg}", flush=True)
                return (True, msg)
            if norm in self._interested_set:
                msg = f"topic '{norm}' already in list"
                print(f"[CMD] {msg}", flush=True)
                return (True, msg)
            self._interested_set.add(norm)
        msg = f"topic '{norm}' added to interested list"
        print(f"[CMD] {msg}", flush=True)
        logger.info(f"Added interested topic: {norm}")
        return (True, msg)

    def remove_interested_topic(self, topic: str) -> tuple:
        """移除一个主题：先 flush，再关闭 writer，返回 (ok, message)

        self.managers 的 key 是原始 ROS topic（带前导 /），而 _interested_set /
        _ros_subscribers 使用 "规范" 形式（去掉前导 /）。这里需要同时尝试两套 key。
        """
        norm = self._normalize_topic(topic)
        flushed = False
        with self._interested_lock:
            if self._interested_set is None:
                self._interested_set = set(self._subscribed_topics)
            if norm not in self._interested_set:
                msg = f"topic '{norm}' not in interested list"
                print(f"[CMD] {msg}", flush=True)
                return (False, msg)
            self._interested_set.discard(norm)

        # self.managers 使用 ROS 原始 topic（如 /sensor/temperature）作为 key
        # 需要尝试两种形式：带 / 和不带 /
        for key in (topic, norm):
            mgr = self.managers.pop(key, None)
            if mgr:
                print(f"[CMD] flushing & closing topic '{key}'...", flush=True)
                mgr.shutdown()
                logger.info(f"Stopped and flushed topic: {key}")
                flushed = True
                break
        else:
            # 两种 key 都没找到 —— manager 可能已被 discovery 清除
            logger.info(f"No manager found for topic '{topic}' (keys tried: {topic}, {norm})")

        # 取消 ROS 订阅
        self._unsubscribe_topic(norm)
        msg = f"topic '{norm}' removed" + (" and flushed" if flushed else "")
        print(f"[CMD] {msg}", flush=True)
        logger.info(f"Removed interested topic: {norm}")
        return (True, msg)

    def start_all_topics(self) -> tuple:
        """开始采集所有主题，返回 (ok, message)"""
        with self._interested_lock:
            self._interested_set = None
            self._failed_subscribe_topics.clear()
        msg = "switched to ALL topics mode"
        print(f"[CMD] {msg}", flush=True)
        logger.info("Switched to all-topics mode")
        return (True, msg)

    def stop_all_topics(self) -> tuple:
        """停止采集所有主题：flush + 关闭所有 manager，返回 (ok, message)"""
        print("[CMD] Stopping all topics...", flush=True)
        with self._interested_lock:
            self._interested_set = set()
        count = 0
        for name, mgr in list(self.managers.items()):
            print(f"[CMD] flushing & closing '{name}'...", flush=True)
            mgr.shutdown()
            count += 1
        self.managers.clear()
        # 取消所有 ROS 订阅
        for norm in list(self._ros_subscribers.keys()):
            self._unsubscribe_topic(norm)
        self._failed_subscribe_topics.clear()
        msg = f"all {count} topics stopped"
        print(f"[CMD] {msg}", flush=True)
        logger.info("All topics stopped")
        return (True, msg)

    def save_topics_to_file(self) -> tuple:
        """保存当前关注主题到 sub_topics.txt，返回 (ok, message, count)"""
        with self._interested_lock:
            if self._interested_set is None:
                topics = list(self._subscribed_topics)
            else:
                topics = sorted(self._interested_set)
        try:
            with open(self._topics_file, 'w') as f:
                for t in topics:
                    f.write(f"/{t}\n")
            msg = f"saved {len(topics)} topics to {self._topics_file}"
            print(f"[CMD] {msg}", flush=True)
            logger.info(f"Saved {len(topics)} topics to {self._topics_file}")
            return (True, msg, len(topics))
        except Exception as e:
            logger.error(f"Failed to save topics: {e}")
            return (False, str(e), 0)

    def delete_topic_from_file(self, topic: str) -> tuple:
        """从 sub_topics.txt 中移除指定 topic，返回 (ok, message)"""
        norm = self._normalize_topic(topic)
        if not os.path.exists(self._topics_file):
            return (True, f"file not found, nothing to delete for '{norm}'")

        try:
            with open(self._topics_file, 'r') as f:
                lines = f.readlines()

            removed = False
            with open(self._topics_file, 'w') as f:
                for line in lines:
                    t = line.strip()
                    if t and not t.startswith('#') and self._normalize_topic(t) == norm:
                        removed = True
                        continue
                    f.write(line)

            if removed:
                msg = f"topic '{norm}' deleted from {self._topics_file}"
                # 同步更新内存中的 interested_set
                with self._interested_lock:
                    if self._interested_set is not None:
                        self._interested_set.discard(norm)
            else:
                msg = f"topic '{norm}' not found in {self._topics_file}"
            print(f"[CMD] {msg}", flush=True)
            logger.info(msg)
            return (True, msg)
        except Exception as e:
            logger.error(f"Failed to delete topic from file: {e}")
            return (False, str(e))

    def _load_topics_from_file(self) -> Optional[set]:
        """从 sub_topics.txt 加载关注主题列表"""
        if not os.path.exists(self._topics_file):
            return None
        try:
            with open(self._topics_file, 'r') as f:
                topics = set()
                for line in f:
                    t = line.strip()
                    if t and not t.startswith('#'):
                        topics.add(self._normalize_topic(t))
            if topics:
                print(f"[Ros_to_TsFile] Loaded {len(topics)} topics from {self._topics_file}", flush=True)
                logger.info(f"Loaded {len(topics)} topics from {self._topics_file}")
                return topics
        except Exception as e:
            logger.error(f"Failed to load topics file: {e}")
        return None

    def _unsubscribe_topic(self, norm_topic: str):
        """取消 ROS 订阅并清理"""
        sub = self._ros_subscribers.pop(norm_topic, None)
        if sub:
            try:
                sub.unregister()
                logger.info(f"Unsubscribed from ROS topic: {norm_topic}")
            except Exception as e:
                logger.warning(f"Failed to unregister subscriber for {norm_topic}: {e}")
        self._subscribed_topics.discard(norm_topic)

    def _try_subscribe(self, topic: str, msg_type: str) -> bool:
        """尝试订阅单个主题，返回是否成功；已订阅或已失败的主题会跳过"""
        norm_topic = self._normalize_topic(topic)
        if norm_topic in self._subscribed_topics:
            return False

        # 跳过之前已尝试且失败的主题，避免重复告警
        if norm_topic in self._failed_subscribe_topics:
            return False

        if not self._is_topic_interested(norm_topic):
            return False

        try:
            msg_class = roslib.message.get_message_class(msg_type)
            if msg_class is None:
                warn_msg = f"Cannot get message class for '{topic}' [{msg_type}] — skipping"
                print(f"[Ros_to_TsFile] {warn_msg}", flush=True)
                logger.warning(warn_msg)
                self._failed_subscribe_topics.add(norm_topic)
                return False
            sub = rospy.Subscriber(topic, msg_class,
                                   lambda m, t=topic, mt=msg_type: self.process_message(t, m, mt))
            self._ros_subscribers[norm_topic] = sub
            self._subscribed_topics.add(norm_topic)
            logger.info(f"Subscribed to {topic} [{msg_type}]")
            return True
        except Exception as e:
            err_msg = f"Failed to subscribe to '{topic}' [{msg_type}]: {e}"
            print(f"[Ros_to_TsFile] {err_msg}", flush=True)
            logger.error(err_msg)
            self._failed_subscribe_topics.add(norm_topic)
            return False

    def _log_subscribed_topics(self, prefix: str = "Currently subscribed topics"):
        """打印当前已订阅的主题列表（同时输出到控制台和日志）"""
        if not self._subscribed_topics:
            print(f"[Ros_to_TsFile] {prefix}: (none)", flush=True)
            logger.info(f"{prefix}: (none)")
            return
        topic_list = sorted(self._subscribed_topics)
        print(f"[Ros_to_TsFile] {prefix} ({len(topic_list)}):", flush=True)
        logger.info(f"{prefix} ({len(topic_list)}):")
        for topic in topic_list:
            print(f"[Ros_to_TsFile]   - /{topic}", flush=True)
            logger.info(f"  - /{topic}")

    def _discover_and_subscribe(self) -> int:
        """发现当前 ROS 图中的主题并订阅匹配项，返回本次新订阅数量"""
        try:
            topics = rospy.get_published_topics()
        except Exception as e:
            logger.warning(f"Failed to get published topics: {e}")
            return 0

        subscribed_now = 0
        already_subscribed = 0
        filtered_out = 0

        for topic, msg_type in topics:
            norm_topic = self._normalize_topic(topic)
            if norm_topic in self._subscribed_topics:
                already_subscribed += 1
                continue
            if not self._is_topic_interested(norm_topic):
                filtered_out += 1
                logger.debug(f"Skipping topic '{topic}' (not in interested list)")
                continue
            if self._try_subscribe(topic, msg_type):
                subscribed_now += 1

        # 使用 print 保证在终端一定可见
        scan_msg = (
            f"Discovery scan: found {len(topics)} topic(s), "
            f"new subscriptions={subscribed_now}, already_subscribed={already_subscribed}, "
            f"filtered_out={filtered_out}, failed={len(self._failed_subscribe_topics)}"
        )
        print(f"[Ros_to_TsFile] {scan_msg}", flush=True)
        logger.info(scan_msg)

        if filtered_out > 0:
            normalized_interested = self._get_normalized_interested()
            if normalized_interested:
                found_names = [self._normalize_topic(t) for t, _ in topics]
                missing = normalized_interested - set(found_names)
                if missing:
                    missing_msg = f"Interested topics not yet found: {sorted(missing)}"
                    print(f"[Ros_to_TsFile] {missing_msg}", flush=True)
                    logger.info(missing_msg)

        if subscribed_now > 0:
            self._log_subscribed_topics()
        return subscribed_now

    def _discovery_loop(self):
        """后台轮询线程：定期发现新主题并订阅"""
        normalized_interested = self._get_normalized_interested()
        if normalized_interested:
            logger.info(f"Discovery loop started, interested topics: {sorted(normalized_interested)}, interval={self.discovery_interval}s")
        else:
            logger.info(f"Discovery loop started, monitoring all topics, interval={self.discovery_interval}s")

        while not self._stop_discovery and not rospy.is_shutdown():
            self._discover_and_subscribe()

            # 如果已订阅所有感兴趣的主题，可以停止发现（可选：继续轮询以发现重新发布的主题）
            if normalized_interested and self._subscribed_topics.issuperset(normalized_interested):
                logger.info("All interested topics are subscribed. Discovery loop will continue monitoring.")
                self._log_subscribed_topics("All subscribed topics")

            # 分阶段睡眠，便于快速响应关闭信号
            slept = 0.0
            while slept < self.discovery_interval and not self._stop_discovery and not rospy.is_shutdown():
                time.sleep(0.5)
                slept += 0.5

    def _ros1_spin(self):
        print(f"[Ros_to_TsFile] Initializing ROS node 'tsfile_bridge_node'...", flush=True)
        rospy.init_node('tsfile_bridge_node', anonymous=True)

        normalized_interested = self._get_normalized_interested()
        if normalized_interested is None:
            print(f"[Ros_to_TsFile] Mode: ALL topics", flush=True)
            logger.info("Mode: ALL topics")
        elif normalized_interested:
            print(f"[Ros_to_TsFile] Interested topics: {sorted(normalized_interested)}", flush=True)
            logger.info(f"Interested topics: {sorted(normalized_interested)}")
        else:
            print(f"[Ros_to_TsFile] Mode: NO topics (waiting for /cmd/{self.bridge_id}/start)", flush=True)
            logger.info(f"Mode: NO topics")

        # 初始等待并订阅已发布的主题
        start_wait = time.time()
        wait_timeout = 30
        initial_topics_found = False
        print(f"[Ros_to_TsFile] Scanning for published topics (timeout {wait_timeout}s)...", flush=True)
        while not rospy.is_shutdown() and time.time() - start_wait < wait_timeout:
            found = self._discover_and_subscribe()
            if found > 0:
                initial_topics_found = True
                break
            print("[Ros_to_TsFile] Waiting for topics...", flush=True)
            logger.info("Waiting for topics...")
            rospy.sleep(2)
        else:
            msg = "No topics found after 30 seconds"
            print(f"[Ros_to_TsFile] {msg}", flush=True)
            logger.warning(msg)
            if not self._get_normalized_interested():
                err = "No topics specified and no topics found. Exiting."
                print(f"[Ros_to_TsFile] {err}", flush=True)
                logger.error(err)
                return

        if not self._subscribed_topics:
            err = f"No topics subscribed. Requested: {self._get_normalized_interested()}"
            print(f"[Ros_to_TsFile] {err}", flush=True)
            logger.error(err)
            return

        self._log_subscribed_topics("Initial subscribed topics")

        # 启动后台发现线程（动态发现新发布的主题）
        self._stop_discovery = False
        self._discovery_thread = threading.Thread(target=self._discovery_loop, daemon=True)
        self._discovery_thread.start()

        # 启动统计线程
        self._stop_stats = False
        self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
        self._stats_thread.start()

        info = (
            f"ROS1 bridge [{self.bridge_id}] started, "
            f"subscribed to {len(self._subscribed_topics)} topic(s). "
            f"Stats every {self.stats_interval}s, discovery every {self.discovery_interval}s."
        )
        print(f"[Ros_to_TsFile] {info}", flush=True)
        logger.info(info)
        if self.mqtt_commander:
            print(f"[Ros_to_TsFile] MQTT commands: /cmd/{self.bridge_id}/{{start,stop,startAll,stopAll,save}}", flush=True)
        rospy.spin()

    def process_bag(self, bag_path: str):
        if rosbag is None:
            logger.error("rosbag module not available. Please install ROS bag support.")
            return
        if not os.path.exists(bag_path):
            logger.error(f"Bag file not found: {bag_path}")
            return

        logger.info(f"Opening bag file: {bag_path}")
        bag = rosbag.Bag(bag_path, 'r')
        topics_to_read = self._get_normalized_interested()
        total_msgs = bag.get_message_count(topic_filters=topics_to_read)
        logger.info(f"Total messages to process: {total_msgs}")

        processed = 0
        start_time = time.time()
        for topic, msg, t in bag.read_messages(topics=topics_to_read):
            msg_type = msg._type if hasattr(msg, '_type') else type(msg).__name__
            self.process_message(topic, msg, msg_type)
            processed += 1
            if processed % self.bag_progress_interval == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                logger.info(f"Progress: {processed}/{total_msgs} msgs ({rate:.1f} msg/s)")

        bag.close()
        elapsed_total = time.time() - start_time
        logger.info(f"Bag processing finished. Processed {processed} messages in {elapsed_total:.2f} seconds.")

    def start(self, bag_path: Optional[str] = None):
        # 启动 MQTT 转发（如已启用）
        if self.mqtt_forwarder:
            self.mqtt_forwarder.start()

        print(f"[Ros_to_TsFile] Bridge ID: {self.bridge_id}", flush=True)
        logger.info(f"Bridge ID: {self.bridge_id}, Starting ROS1 TsFile Bridge, data root: {self.root_path}, storage_mode={self.storage_mode}, compress_blob={self.compress_blob}")
        if bag_path:
            self.process_bag(bag_path)
        else:
            self._ros1_spin()

    def shutdown(self):
        self._stop_stats = True
        self._stop_discovery = True
        if self._discovery_thread and self._discovery_thread.is_alive():
            self._discovery_thread.join(timeout=2.0)
        if self._stats_thread and self._stats_thread.is_alive():
            self._stats_thread.join(timeout=2.0)
        logger.info("Shutting down bridge...")
        # 关闭 MQTT 命令控制
        if self.mqtt_commander:
            self.mqtt_commander.shutdown()
        # 关闭 MQTT 转发
        if self.mqtt_forwarder:
            self.mqtt_forwarder.shutdown()
        for mgr in self.managers.values():
            mgr.shutdown()
        logger.info("Bridge shutdown complete")
        for handler in logging.getLogger().handlers:
            handler.flush()
        logging.shutdown()


def signal_handler(bridge):
    def handler(signum, frame):
        print(f"\n[Ros_to_TsFile] Received signal {signum}, shutting down gracefully...", flush=True)
        logger.info(f"Received signal {signum}, shutting down...")
        for h in logging.getLogger().handlers:
            h.flush()
        bridge.shutdown()
        for h in logging.getLogger().handlers:
            h.flush()
        logging.shutdown()
        print("[Ros_to_TsFile] Shutdown complete.", flush=True)
        sys.exit(0)
    return handler


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ROS1 to TsFile Bridge with configurable storage mode (embedded/external)")
    parser.add_argument("--root_path", type=str, default="./tsfile-viewer/data/ros_data_assets", help="Data storage root directory")
    parser.add_argument("--buffer_size", type=int, default=10, help="Number of rows to buffer before flushing (default 100)")
    parser.add_argument("--max_file_size_mb", type=int, default=100, help="Max TsFile and binary data file size in MB before rotation")
    parser.add_argument("--topics", nargs="*", help="List of topics to subscribe (default: all discovered)")
    parser.add_argument("--no_msg_stamp", action="store_true", help="Use system time instead of ROS message stamp")
    parser.add_argument("--flush_interval", type=float, default=0.5, help="Periodic flush interval in seconds (default 0.5)")
    parser.add_argument("--bag", type=str, help="Process a ROS bag file instead of subscribing to live topics")
    parser.add_argument("--stats_interval", type=float, default=10.0, help="Live mode statistics print interval (seconds, default 10)")
    parser.add_argument("--bag_progress_interval", type=int, default=10000, help="Bag processing progress print interval (messages, default 10000)")
    parser.add_argument("--storage_mode", type=str, choices=["embedded", "external"], default="embedded",
                        help="Storage mode for image/pointcloud data: 'embedded' (store in TsFile as STRING column, raw bytes) or 'external' (store compressed in external files, TsFile stores index). Default: embedded")
    parser.add_argument("--compress_blob", action="store_true",
                        help="Compress image/pointcloud data with gzip. In embedded mode, compresses then stores as STRING; in external mode, compresses before appending to external file. Default: False")
    parser.add_argument("--discovery_interval", type=float, default=5.0,
                        help="Interval in seconds for dynamic topic discovery loop (default 5.0)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose (DEBUG) console output")
    parser.add_argument("--mqtt_host", type=str, default=None,
                        help="MQTT broker host (enables MQTT forwarding of message metadata)")
    parser.add_argument("--mqtt_port", type=int, default=1883,
                        help="MQTT broker port (default 1883)")
    parser.add_argument("--mqtt_root_topic", type=str, default="ros",
                        help="Root topic prefix for MQTT forwarding (default 'ros')")
    parser.add_argument("--bridge_id", type=str, default=None,
                        help="Bridge unique ID for MQTT command control (default: bridge_<hostname>_<pid>)")

    args = parser.parse_args()

    if args.verbose:
        console_handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled")

    print(f"[Ros_to_TsFile] Starting with args: {args}", flush=True)
    logger.info(f"Starting with args: {args}")

    bridge = RosToTsFileBridge(
        root_path=args.root_path,
        default_buffer_size=args.buffer_size,
        max_file_size_mb=args.max_file_size_mb,
        interested_topics=args.topics,
        use_msg_stamp=not args.no_msg_stamp,
        flush_interval=args.flush_interval,
        stats_interval=args.stats_interval,
        bag_progress_interval=args.bag_progress_interval,
        storage_mode=args.storage_mode,
        compress_blob=args.compress_blob,
        discovery_interval=args.discovery_interval,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_root_topic=args.mqtt_root_topic,
        bridge_id=args.bridge_id
    )

    if args.bag:
        bridge.start(bag_path=args.bag)
        bridge.shutdown()
    else:
        signal.signal(signal.SIGINT, signal_handler(bridge))
        signal.signal(signal.SIGTERM, signal_handler(bridge))
        try:
            bridge.start()
        except KeyboardInterrupt:
            bridge.shutdown()
