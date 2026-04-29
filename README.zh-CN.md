# Bag2RawSync

`Bag2RawSync` 是一个用于离线读取 ROS `.bag` 文件的轻量级 Python 工具，重点解决两件事：

- 提取多传感器原始数据
- 生成按同一时间轴对齐的结果表

它适合以下场景：

- 导出左右相机图像
- 导出 Livox 激光雷达帧
- 导出 IMU 和 GNSS/RTK 数据
- 生成后续处理可直接使用的对齐 CSV

这个工具不依赖完整 ROS 桌面环境，只需要 Python 即可运行。

## 仓库结构

```text
bag2rawsync/
  .github/
    workflows/
      smoke-test.yml
  .gitignore
  .gitattributes
  README.md
  README.zh-CN.md
  requirements.txt
  scripts/
    bag2rawsync.py
```

## 功能概览

- 无需安装完整 ROS 即可读取 bag
- 一次遍历导出全部 topic
- 压缩图像 topic 导出为原始图像文件
- Livox 雷达导出为 `.npz`
- IMU 导出为 `imu.csv`
- `NavSatFix` 导出为 `navsatfix.csv`
- 其它自定义消息导出为 `messages.jsonl`
- 为每个 topic 生成 `manifest.json`
- 可选保存 ROS 原始序列化后的 `.bin`
- 可选生成统一时间轴对齐结果
- 自带 GitHub Actions 基础冒烟检查

## 环境要求

- Python 3.10 或更新版本
- 推荐在 Windows PowerShell 中运行

当前已验证环境：

- Python 3.13

## 安装依赖

在仓库根目录执行：

```powershell
python -m pip install -r .\requirements.txt
```

依赖只有：

- `numpy>=2.0`
- `rosbags>=0.11.0`

不需要安装：

- `rosbag`
- `ros2`
- 完整 ROS 桌面环境

## 快速开始

### 1. 仅查看 bag 中有哪些 topic

```powershell
python .\scripts\bag2rawsync.py --list-only "D:\data\demo.bag"
```

### 2. 查看工具版本

```powershell
python .\scripts\bag2rawsync.py --version
```

### 3. 导出全部原始数据

```powershell
python .\scripts\bag2rawsync.py "D:\data\demo.bag"
```

### 4. 指定输出目录

```powershell
python .\scripts\bag2rawsync.py --output "D:\output\demo_extract" "D:\data\demo.bag"
```

### 5. 覆盖已有输出目录

```powershell
python .\scripts\bag2rawsync.py --overwrite --output "D:\output\demo_extract" "D:\data\demo.bag"
```

### 6. 额外保存原始序列化消息

```powershell
python .\scripts\bag2rawsync.py --save-serialized "D:\data\demo.bag"
```

## 时间对齐

生成统一时间轴对齐表：

```powershell
python .\scripts\bag2rawsync.py --align "D:\data\demo.bag"
```

手动指定主时间轴 topic：

```powershell
python .\scripts\bag2rawsync.py --align --align-anchor "/livox/lidar" "D:\data\demo.bag"
```

默认主时间轴优先级：

1. `/livox/lidar`
2. `/camera_agent/img_left/compressed`
3. `/camera_agent/img_right/compressed`

对齐逻辑：

- 优先使用 `header.stamp`
- 如果没有 `header.stamp`，退回使用 bag 记录时间
- 普通 topic 使用最近邻匹配
- IMU 会额外输出当前锚点区间的样本数量和起止范围

## 输出结构

```text
<bag_stem>_extracted/
  summary.json
  extract_stats.json
  topics/
    camera_agent/
      img_left/
        compressed/
          data/
          index.csv
          manifest.json
    livox/
      lidar/
        data/
        index.csv
        manifest.json
      imu/
        imu.csv
        manifest.json
    rtk_agent/
      navsatfix/
        navsatfix.csv
        manifest.json
    sharevins/
      shot/
        messages.jsonl
        manifest.json
  alignment/
    livox__lidar_aligned.csv
    livox__lidar_aligned_manifest.json
```

## 主要输出文件说明

### `summary.json`

记录整个 bag 的总体信息：

- bag 路径
- 开始时间
- 结束时间
- 时长
- topic 列表
- 每个 topic 的消息数量

### `extract_stats.json`

记录整个导出任务的统计结果：

- topic 数量
- 总消息数
- 每个 topic 的输出目录
- 每个 topic 生成了哪些文件
- 如果启用 `--align`，还会记录对齐输出信息

### `manifest.json`

每个 topic 单独一份，记录：

- topic 名称
- 消息类型
- 消息数量
- bag 时间范围
- header 时间范围
- 生成的文件类型

### `index.csv`

用于图像和 Livox 雷达 topic 的索引表，常见字段包括：

- `seq_idx`
- `bag_timestamp_ns`
- `header_stamp_ns`
- `file`
- `point_num`
- `timebase_ns`

### `imu.csv`

IMU 的逐样本导出，包含：

- 时间戳
- 角速度
- 线加速度
- 姿态四元数

### `navsatfix.csv`

GNSS/RTK 导出，包含：

- 经纬高
- 定位状态
- 协方差类型

### `messages.jsonl`

用于保存未单独做格式化导出的自定义 topic。

每一行都是一个完整 JSON，通常包含：

- `seq_idx`
- 时间戳
- `topic`
- `msgtype`
- `message`

## 时间戳说明

bag 中通常会出现两类时间：

- `bag timestamp`：消息被写入 bag 的时间
- `header.stamp`：消息头里的采样时间

工具会尽量同时保留这两种时间。

在做对齐时：

- 优先使用 `header.stamp`
- 只有在缺失时才使用 `bag timestamp`

如果不同 topic 不在同一套时间基准上，对齐结果里出现较大的时间差是正常现象，通常说明源数据本身没有完全同步，而不一定是工具错误。

## 示例命令

导出全部原始数据并生成对齐结果：

```powershell
python .\scripts\bag2rawsync.py --overwrite --align "D:\data\demo.bag"
```

导出全部原始数据、保存原始序列化消息，并生成对齐结果：

```powershell
python .\scripts\bag2rawsync.py --overwrite --save-serialized --align "D:\data\demo.bag"
```
