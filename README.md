# ROS1 → TsFile 桥接工具

将 ROS1 中的传感器消息（图像、点云）及通用消息（包括图谱等自定义类型）写入 TsFile 时序数据库格式，并对图像/点云数据采用 **gzip 压缩 + 聚合存储 + 文件轮转** 策略，在 TsFile 中保存精确的文件偏移索引，同时自动保存表结构（`schema.json`）便于后续解析。

---

## 功能特性

- ✅ **实时订阅 ROS 主题** 或 **离线回放 rosbag**
- ✅ **图像（`sensor_msgs/Image`）**：原始数据 gzip 压缩后追加到主题目录下的 `data/YYYYMMDD_HHMMSS_纳秒.bin` 文件；TsFile 中存储文件路径、偏移量、压缩大小、原始大小、header 信息（seq, stamp, frame_id）、编码、宽高等元数据
- ✅ **点云（`sensor_msgs/PointCloud2`）**：同样压缩追加到 `data/*.bin`；TsFile 中存储索引信息及点云字段描述、点数、header 等
- ✅ **通用消息**（包括自定义图谱消息）：动态展平所有字段，作为普通时序数据写入 TsFile（不创建外部二进制文件）
- ✅ **文件自动轮转**：每个主题的 TsFile 和二进制数据文件均可独立限制大小（默认 100 MB），超出后自动创建新文件
- ✅ **批量写入 + 定时刷新**：提高写入效率，防止数据积压
- ✅ **Schema 持久化**：每个主题目录下自动生成 `schema.json`，记录表名和列定义，便于离线解析
- ✅ **MQTT 转发**：支持将 ROS 消息元数据以 JSON 格式实时转发到 MQTT broker，可用于数据可视化、远程监控等场景
- ✅ **线程安全**：支持多主题并发写入，文件轮转和追加操作均加锁保护

---

## 环境要求

- **操作系统**：Ubuntu 16.04 / 18.04 / 20.04（ROS Kinetic/Melodic/Noetic）
- **ROS 版本**：ROS1（完整安装，包含 `rospy`, `roslib`, `rosbag` 等）
- **Python 版本**：Python 3.6+
- **依赖库**：
  - `tsfile` Python SDK（需自行编译，详见下文）
  - `paho-mqtt`（MQTT 转发功能需要，可通过 `pip install paho-mqtt` 安装）
  - 标准库：`gzip`, `threading`, `json`, `tempfile`, `base64`（无需额外安装）

---

## 安装与配置

### 1. 安装 TsFile Python SDK

目前 TsFile 为 Apache IoTDB 的子项目，Python 绑定需手动编译：

```bash
git clone https://github.com/apache/iotdb.git
cd iotdb/tsfile
# 编译 C++ 动态库并生成 Python 绑定（具体参考官方文档）
```

将生成的 `tsfile.so` 或 `tsfile.py` 放置于 `PYTHONPATH` 中，并确保 `libtsfile.so` 在 `LD_LIBRARY_PATH` 中：

```bash
export LD_LIBRARY_PATH=/path/to/tsfile/lib:$LD_LIBRARY_PATH
export PYTHONPATH=/path/to/tsfile/python:$PYTHONPATH
```

### 2. 下载本工具

```bash
git clone <repository_url>
cd ros_to_tsfile
```

---

## 使用方法

### 实时订阅模式

```bash
# 订阅所有出现的主题（自动识别 Image/PointCloud2）
python ros_to_tsfile.py

# 只订阅指定主题
python ros_to_tsfile.py --topics /camera/image_raw /lidar/points

# 自定义存储根目录和文件大小限制
python ros_to_tsfile.py --root_path /data/ros_ts --max_file_size_mb 200
```

### 离线回放 rosbag

```bash
# 处理整个 bag 文件
python ros_to_tsfile.py --bag my_data.bag

# 只处理 bag 中的部分主题
python ros_to_tsfile.py --bag my_data.bag --topics /camera/image_raw /lidar/points

# 使用系统时间戳（忽略消息自带时间戳）
python ros_to_tsfile.py --bag my_data.bag --no_msg_stamp
```

### 启用 MQTT 转发

```bash
# 启动 bridge 并转发到本地 MQTT broker
python ros_to_tsfile.py --mqtt_host localhost

# 自定义 MQTT 端口和根主题前缀
python ros_to_tsfile.py --mqtt_host 192.168.1.100 --mqtt_port 1883 --mqtt_root_topic robot_01

# 结合 bag 回放 + MQTT 转发
python ros_to_tsfile.py --bag my_data.bag --mqtt_host localhost --mqtt_root_topic replay/ros
```

> **注意**：若不指定 `--mqtt_host`，MQTT 转发功能不会启用，不影响 TsFile 写入。

### MQTT 远程命令控制

指定 `--bridge_id` 后，bridge 会订阅 `/cmd/{bridge_id}/#` 接收远程命令，支持运行时动态管理采集主题。

```bash
# 启动 bridge 并开启命令控制
python ros_to_tsfile.py --mqtt_host localhost --bridge_id robot_01
```

**命令列表：**

| 命令 | Payload | 功能 |
|------|---------|------|
| `/cmd/{id}/start` | `{"topic": "/camera/image_raw"}` | 添加主题到关注列表，discovery 自动订阅 |
| `/cmd/{id}/stop` | `{"topic": "/camera/image_raw"}` | flush 数据 → 关闭 writer → 停止采集 |
| `/cmd/{id}/delete` | `{"topic": "/camera/image_raw"}` | 停止采集 + 从 `sub_topics.txt` 中永久移除 |
| `/cmd/{id}/startAll` | `{}` | 订阅全部 ROS 主题（恢复默认） |
| `/cmd/{id}/stopAll` | `{}` | flush 全部 → 关闭所有 writer → 清空列表 |
| `/cmd/{id}/save` | `{}` | 保存当前关注主题到 `sub_topics.txt`（下次启动自动加载） |

**远程控制示例：**

```bash
# 停止所有采集
mosquitto_pub -t "/cmd/robot_01/stopAll" -m '{}'

# 开始采集相机图像
mosquitto_pub -t "/cmd/robot_01/start" -m '{"topic":"/camera/image_raw"}'

# 持久化当前配置
mosquitto_pub -t "/cmd/robot_01/save" -m '{}'

# 恢复采集全部
mosquitto_pub -t "/cmd/robot_01/startAll" -m '{}'
```

> **持久化**：启动时加载顺序 `--topics` 参数 > `sub_topics.txt` > 全部主题。可通过 `/cmd/{id}/save` 随时保存当前关注列表。

### 查看帮助

```bash
python ros_to_tsfile.py --help
```

---

## 命令行参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--root_path` | str | `ros_data_assets` | 数据存储根目录 |
| `--buffer_size` | int | 500 | TsFile 内存缓冲行数，达到此值即刷新到磁盘 |
| `--max_file_size_mb` | int | 100 | 单个 TsFile 或二进制数据文件的最大大小（MB） |
| `--topics` | list[str] | None | 要订阅/处理的主题列表，不指定则处理所有发现的主题 |
| `--no_msg_stamp` | flag | False | 若指定，则使用系统当前时间作为时间戳，否则使用消息 header 中的 stamp |
| `--flush_interval` | float | 0.5 | 定时刷新间隔（秒），防止高频数据滞留 |
| `--bag` | str | None | 指定 rosbag 文件路径，进入离线回放模式 |
| `--stats_interval` | float | 10.0 | 实时模式下统计信息打印间隔（秒） |
| `--bag_progress_interval` | int | 10000 | 回放模式时每处理多少条消息打印一次进度 |
| `--storage_mode` | str | `embedded` | 图像/点云存储模式：`embedded`（Base64 编码存入 TsFile）或 `external`（外部 `.bin` 文件 + TsFile 索引） |
| `--compress_blob` | flag | False | 对图像/点云数据进行 gzip 压缩 |
| `--discovery_interval` | float | 5.0 | 动态主题发现扫描间隔（秒） |
| `--verbose` | flag | False | 开启 DEBUG 级别控制台日志输出 |
| `--mqtt_host` | str | None | MQTT broker 地址，不指定则不启用 MQTT 转发 |
| `--mqtt_port` | int | 1883 | MQTT broker 端口 |
| `--mqtt_root_topic` | str | `ros` | MQTT 根主题前缀，ROS 主题会挂载到此前缀下 |
| `--bridge_id` | str | `bridge_<host>_<pid>` | Bridge 唯一 ID，用于 MQTT 命令控制 `/cmd/{id}/#` |

---

## 数据存储结构

假设根目录为 `ros_data_assets`，主题名为 `/camera/image_raw`，则目录结构如下：

```
ros_data_assets/
└── camera_image_raw/                     # 清理后的主题名（特殊字符替换为 _）
    ├── schema.json                       # 表结构定义（JSON）
    ├── 20250326_120000_1234567890.tsfile # TsFile 索引文件（多个）
    ├── 20250326_130000_1234567891.tsfile
    └── data/                             # 二进制数据文件目录（仅图像/点云主题）
        ├── 20260420_000006_1776614406506392876.bin
        ├── 20260420_001002_1776614406506392880.bin
        └── ...
```

### schema.json 示例（图像主题）

```json
{
  "table_name": "camera_image_raw",
  "columns": [
    {"name": "file_path", "data_type": "STRING", "category": "FIELD"},
    {"name": "offset", "data_type": "INT64", "category": "FIELD"},
    {"name": "compressed_size", "data_type": "INT64", "category": "FIELD"},
    {"name": "uncompressed_size", "data_type": "INT64", "category": "FIELD"},
    {"name": "header_seq", "data_type": "INT64", "category": "FIELD"},
    {"name": "header_stamp_secs", "data_type": "INT64", "category": "FIELD"},
    {"name": "header_stamp_nsecs", "data_type": "INT64", "category": "FIELD"},
    {"name": "header_frame_id", "data_type": "STRING", "category": "FIELD"},
    {"name": "encoding", "data_type": "STRING", "category": "FIELD"},
    {"name": "width", "data_type": "INT32", "category": "FIELD"},
    {"name": "height", "data_type": "INT32", "category": "FIELD"},
    {"name": "step", "data_type": "INT32", "category": "FIELD"},
    {"name": "is_bigendian", "data_type": "INT32", "category": "FIELD"}
  ]
}
```

### TsFile 中的记录（图像索引）

| 时间戳 | file_path | offset | compressed_size | uncompressed_size | encoding | width | height | header_seq | header_frame_id | ... |
|--------|-----------|--------|-----------------|-------------------|----------|-------|--------|------------|----------------|-----|
| 123456 | data/20260420_000006_1776614406506392876.bin | 0 | 10240 | 524288 | rgb8 | 640 | 480 | 42 | "camera_link" | ... |

> **注意**：TsFile 的时间戳列即为从消息 header 中提取的 `stamp`（毫秒）。

### 通用消息存储

通用消息的所有字段直接作为 TsFile 的列存储，不产生外部二进制文件。例如一个图谱消息被展平为 `nodes`, `edges`, `timestamp` 等列，每行对应一条消息。

---

## MQTT 转发

启用 `--mqtt_host` 后，每条 ROS 消息的元数据会以 JSON 格式发布到对应的 MQTT 主题。**原始二进制数据（图像像素、点云坐标）不会通过 MQTT 发送**，仅发送元数据，保证消息轻量。

### 主题映射规则

ROS 主题名去掉首尾 `/` 后，挂载到 `--mqtt_root_topic` 指定的根前缀下：

| ROS 主题 | `--mqtt_root_topic` | MQTT 主题 |
|----------|---------------------|-----------|
| `/camera/image_raw` | `ros`（默认） | `ros/camera/image_raw` |
| `/lidar/points` | `robot_01` | `robot_01/lidar/points` |
| `/gazebo/model_states` | `replay/ros` | `replay/ros/gazebo/model_states` |

### 消息格式

#### Image（sensor_msgs/Image）

```json
{
  "type": "sensor_msgs/Image",
  "header_seq": 123,
  "header_stamp_secs": 1680000000,
  "header_stamp_nsecs": 500000000,
  "header_frame_id": "camera_link",
  "encoding": "rgb8",
  "width": 640,
  "height": 480,
  "step": 1920,
  "is_bigendian": 0
}
```

#### PointCloud2（sensor_msgs/PointCloud2）

```json
{
  "type": "sensor_msgs/PointCloud2",
  "header_seq": 456,
  "header_stamp_secs": 1680000000,
  "header_stamp_nsecs": 600000000,
  "header_frame_id": "lidar_link",
  "num_points": 120000,
  "fields": "x:7:0;y:7:4;z:7:8;intensity:7:16",
  "is_bigendian": 0,
  "point_step": 32,
  "row_step": 3840000,
  "is_dense": 1
}
```

#### 通用消息

```json
{
  "type": "std_msgs/String",
  "data": "hello world"
}
```

> 展平后的字段中，超过 512 字节的二进制字段会被替换为空字符串（避免 JSON 膨胀），原始二进制数据仍需从 TsFile 读取。

### 监听示例

```bash
# 监听所有转发的消息
mosquitto_sub -t 'ros/#' -v

# 只监听图像主题
mosquitto_sub -t 'ros/camera/image_raw' -v

# 使用 Python 订阅
python -c "
import paho.mqtt.client as mqtt
import json

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    print(f'[{msg.topic}] {data[\"type\"]} seq={data.get(\"header_seq\", \"-\")}')

client = mqtt.Client()
client.on_message = on_message
client.connect('localhost')
client.subscribe('ros/#')
client.loop_forever()
"
```

---

## 读取数据示例

### 读取图像索引并解压原始数据

```python
import os
import gzip
from tsfile import TsFileReader

tsfile_path = "ros_data_assets/camera_image_raw/20250326_120000_1234567890.tsfile"
reader = TsFileReader(tsfile_path)

rows = reader.query(
    "select file_path, offset, compressed_size, header_frame_id "
    "from root.camera_image_raw where time >= 1000 and time <= 2000"
)

for row in rows:
    data_file = os.path.join("ros_data_assets/camera_image_raw", row.file_path)
    with open(data_file, "rb") as f:
        f.seek(row.offset)
        compressed = f.read(row.compressed_size)
        original = gzip.decompress(compressed)
        # original 即为 sensor_msgs/Image 中的 data 字段（原始字节流）
        print(f"Frame from {row.header_frame_id}, size={len(original)}")
```

### 读取通用消息

```python
rows = reader.query("select nodes, edges from root.my_graph_topic where time > 1000")
for row in rows:
    print(row.nodes, row.edges)
```

### 通过 schema.json 获取表结构

```python
import json
with open("ros_data_assets/camera_image_raw/schema.json") as f:
    schema = json.load(f)
    for col in schema["columns"]:
        print(col["name"], col["data_type"])
```

---

## 性能与调优建议

- **缓冲区大小** (`--buffer_size`)：较大的值可减少磁盘 I/O 次数，但会增加内存占用。对于高频主题（如 30Hz 图像），推荐 500~1000。
- **文件大小限制** (`--max_file_size_mb`)：TsFile 和二进制数据文件独立轮转。二进制文件过大可能导致单次读取延迟增加，建议 100~500 MB。
- **定时刷新间隔** (`--flush_interval`)：若消息量极大且对实时性要求高，可减小至 0.1 秒；若允许几秒延迟，可增大以减少磁盘压力。
- **压缩级别**：代码中固定使用 `gzip.compress(..., compresslevel=9)`，压缩率最高但稍慢。可修改为 6 以平衡性能。
- **多主题并发**：每个主题独立写入，相互不影响。如需限制总体资源，可通过操作系统 ulimit 或 cgroup 控制。

---

## 常见问题

### Q1: 导入 `tsfile` 失败，提示找不到动态库？
**A:** 确保 `libtsfile.so` 在 `LD_LIBRARY_PATH` 中，例如：
```bash
export LD_LIBRARY_PATH=/path/to/tsfile/lib:$LD_LIBRARY_PATH
```

### Q2: 图像/点云数据文件轮转后，旧的 TsFile 还能正确读取吗？
**A:** 可以。TsFile 中存储的 `file_path` 是相对路径（如 `data/20260420_000006_1776614406506392876.bin`），只要主题目录结构不变，即可通过偏移量正确定位。

### Q3: 通用消息展平时，列表/数组被截断为字符串，如何保留完整结构？
**A:** 当前实现将超过 20 个元素的列表转为 `str(list[:20])`。如需完整保存，可修改 `_flatten_msg` 中的逻辑，例如使用 JSON 序列化后存入 STRING 字段。

### Q4: 能否支持 ROS2？
**A:** 当前版本仅支持 ROS1。若需 ROS2 支持，需替换 `rospy` 为 `rclpy`，并适配消息类型。

### Q5: 停止程序时数据会丢失吗？
**A:** 程序捕获 SIGINT/SIGTERM 信号，会刷新所有缓冲数据并关闭文件。直接 `kill -9` 可能导致未刷新数据丢失。

### Q6: 为什么数据文件很多（如 `20260420_000006_1776614406506392876.bin` 多个）？
**A:** 每个主题独立管理数据文件，当单个文件大小超过 `--max_file_size_mb` 时自动轮转。若文件数量过多，可调大该参数（如 `--max_file_size_mb 500`）。另外，程序每次重启会创建新文件，不会追加到旧文件。


### Q7: `is_bigendian` 为什么要在 TsFile 中存储它？**  
`is_bigendian` 是 ROS 传感器消息中用于表示**字节序**的字段。

- **对于图像 (`sensor_msgs/Image`)**：指示图像数据的像素字节序。在大多数常见平台（如 x86、ARM）上为小端（`is_bigendian = False`），但在某些嵌入式或网络传输场景下可能为大端。存储该字段可以确保后续解码图像数据时使用正确的字节顺序，避免颜色通道错乱。

- **对于点云 (`sensor_msgs/PointCloud2`)**：指示点云数据中每个字段（如 x, y, z, intensity 等）的字节序。同样，在跨平台或回放数据时，知道原始字节序是正确解析点云坐标和属性的必要条件。

**为什么要在 TsFile 中存储它？**  
因为图像和点云的原始数据被压缩后以二进制块存储（不包含 ROS 消息头），而 `is_bigendian` 作为元数据保存在 TsFile 的索引记录中，使得读取时能够正确解释二进制数据块。没有这个字段，若在不同字节序的机器上读取数据，可能得到错误的值。

简言之，`is_bigendian` 保证了数据的**平台无关可移植性**。

---

## 许可证

Apache 2.0

---

## 联系与贡献

欢迎提交 Issue 或 Pull Request。

**Happy ROS + TsFile！**
