# Bag2RawSync

`Bag2RawSync` is a lightweight Python tool for extracting raw multi-sensor data from ROS `.bag` files and generating a same-timeline alignment table.

Chinese documentation: [README.zh-CN.md](README.zh-CN.md)
Change history: [CHANGELOG.md](CHANGELOG.md)

It is designed for offline workflows such as:

- exporting left/right camera frames
- exporting Livox lidar frames
- exporting IMU and GNSS/RTK streams
- building a sensor-alignment CSV for downstream processing

The tool does not require a full ROS desktop installation. A normal Python environment is enough.

## Repository Layout

```text
bag2rawsync/
  .gitignore
  README.md
  requirements.txt
  scripts/
    bag2rawsync.py
```

## Features

- Read ROS bag files without installing full ROS
- Export all topics in one pass
- Save compressed image topics as raw image files
- Save Livox lidar frames as `.npz`
- Save IMU messages as `imu.csv`
- Save `NavSatFix` messages as `navsatfix.csv`
- Save other custom messages as `messages.jsonl`
- Generate per-topic `manifest.json`
- Optionally save serialized ROS payloads as `.bin`
- Optionally generate a same-timeline alignment CSV

## Requirements

- Python 3.10 or newer
- Windows PowerShell is recommended for the examples below

Validated locally with Python 3.13.

## Installation

```powershell
python -m pip install -r .\requirements.txt
```

Dependencies:

- `numpy>=2.0`
- `rosbags>=0.11.0`

You do not need:

- `rosbag`
- `ros2`
- a full ROS desktop environment

## Quick Start

### List topics only

```powershell
python .\scripts\bag2rawsync.py --list-only "D:\data\demo.bag"
```

### Export all raw data

```powershell
python .\scripts\bag2rawsync.py "D:\data\demo.bag"
```

### Export to a custom directory

```powershell
python .\scripts\bag2rawsync.py --output "D:\output\demo_extract" "D:\data\demo.bag"
```

### Overwrite an existing output directory

```powershell
python .\scripts\bag2rawsync.py --overwrite --output "D:\output\demo_extract" "D:\data\demo.bag"
```

### Save serialized ROS messages too

```powershell
python .\scripts\bag2rawsync.py --save-serialized "D:\data\demo.bag"
```

## Alignment Mode

Generate a same-timeline alignment table:

```powershell
python .\scripts\bag2rawsync.py --align "D:\data\demo.bag"
```

Specify an anchor topic manually:

```powershell
python .\scripts\bag2rawsync.py --align --align-anchor "/livox/lidar" "D:\data\demo.bag"
```

Default anchor preference:

1. `/livox/lidar`
2. `/camera_agent/img_left/compressed`
3. `/camera_agent/img_right/compressed`

Alignment behavior:

- Prefer `header.stamp`
- Fallback to bag write timestamp when `header.stamp` is missing
- Use nearest-neighbor matching for normal topics
- Add IMU window statistics for each anchor interval

## Output Layout

After a normal export:

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

## File Types

### `summary.json`

Bag-level summary:

- bag path
- start/end time
- duration
- topic list
- message counts

### `extract_stats.json`

Export-level summary:

- topic count
- total message count
- per-topic output location
- produced artifact types
- alignment output metadata when `--align` is enabled

### `manifest.json`

Per-topic metadata:

- topic name
- message type
- message count
- bag timestamp range
- header timestamp range
- generated artifacts

### `index.csv`

Used for image and Livox lidar topics.

Typical fields:

- `seq_idx`
- `bag_timestamp_ns`
- `header_stamp_ns`
- `file`
- topic-specific fields such as `point_num` and `timebase_ns`

### `imu.csv`

Per-sample IMU export with:

- timestamp
- angular velocity
- linear acceleration
- quaternion orientation

### `navsatfix.csv`

GNSS/RTK export with:

- latitude / longitude / altitude
- fix status
- covariance type

### `messages.jsonl`

Used for custom topics that do not have a dedicated exporter.

Each line is a complete JSON object that includes:

- `seq_idx`
- timestamps
- `topic`
- `msgtype`
- `message`

## Timestamp Notes

Two time domains may appear in the same bag:

- `bag timestamp`: when the message was written into the bag
- `header.stamp`: when the sensor message says it was sampled

The tool keeps both when possible.

For alignment:

- `header.stamp` is preferred
- `bag timestamp` is only used as fallback

If different topics are not synchronized to the same clock, large time deltas in the alignment CSV are expected. That usually reflects the source data rather than a bug in the tool.

## Example Commands

Export all data and generate alignment:

```powershell
python .\scripts\bag2rawsync.py --overwrite --align "D:\data\demo.bag"
```

Export all data, save serialized payloads, and generate alignment:

```powershell
python .\scripts\bag2rawsync.py --overwrite --save-serialized --align "D:\data\demo.bag"
```

## Versioning

This repository uses lightweight semantic versioning for tool snapshots.

- `v0.1.0`: initial public release of the extraction and alignment workflow
