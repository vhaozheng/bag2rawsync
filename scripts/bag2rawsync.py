#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import bisect
import csv
import json
import re
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
from rosbags.highlevel import AnyReader


COMPRESSED_IMAGE = "sensor_msgs/msg/CompressedImage"
IMU_MSG = "sensor_msgs/msg/Imu"
NAVSATFIX_MSG = "sensor_msgs/msg/NavSatFix"
LIVOX_CUSTOM_MSG = "livox_ros_driver2/msg/CustomMsg"
DEFAULT_ALIGNMENT_ANCHORS = [
    "/livox/lidar",
    "/camera_agent/img_left/compressed",
    "/camera_agent/img_right/compressed",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: Iterable[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def topic_key(topic: str) -> str:
    parts = [sanitize_part(part) for part in topic.strip("/").split("/") if part]
    return "__".join(parts) if parts else "_root"


def sanitize_part(part: str) -> str:
    part = part.strip()
    if not part:
        return "_"
    return re.sub(r"[^0-9A-Za-z._-]+", "_", part)


def topic_dir(base_dir: Path, topic: str) -> Path:
    parts = [sanitize_part(part) for part in topic.strip("/").split("/") if part]
    return base_dir.joinpath(*parts) if parts else base_dir / "_root"


def maybe_stamp_to_ns(stamp: object | None) -> int | None:
    if stamp is None or not hasattr(stamp, "sec") or not hasattr(stamp, "nanosec"):
        return None
    return int(getattr(stamp, "sec")) * 1_000_000_000 + int(getattr(stamp, "nanosec"))


def message_header_stamp_ns(msg: object) -> int | None:
    header = getattr(msg, "header", None)
    return maybe_stamp_to_ns(getattr(header, "stamp", None))


def effective_timestamp_ns(bag_timestamp_ns: int, header_stamp_ns: int | None) -> int:
    return int(header_stamp_ns) if header_stamp_ns is not None else int(bag_timestamp_ns)


def effective_timestamp_source(header_stamp_ns: int | None) -> str:
    return "header" if header_stamp_ns is not None else "bag"


def to_plain_object(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return {
            "__kind__": "bytes",
            "size": len(raw),
            "base64": base64.b64encode(raw).decode("ascii"),
        }
    if isinstance(value, (list, tuple)):
        return [to_plain_object(item) for item in value]
    if hasattr(value, "tolist") and not hasattr(value, "__msgtype__"):
        return value.tolist()
    if hasattr(value, "__dict__"):
        result: dict[str, Any] = {}
        msgtype = getattr(value, "__msgtype__", None)
        if msgtype:
            result["__msgtype__"] = msgtype
        for key, item in vars(value).items():
            if key == "__msgtype__":
                continue
            result[key] = to_plain_object(item)
        return result
    return str(value)


def build_summary(reader: AnyReader, bag: Path) -> dict[str, Any]:
    return {
        "bag": str(bag),
        "start_time_ns": int(reader.start_time),
        "end_time_ns": int(reader.end_time),
        "duration_s": float((reader.end_time - reader.start_time) / 1e9),
        "topics": [
            {
                "topic": connection.topic,
                "msgtype": connection.msgtype,
                "count": connection.msgcount,
            }
            for connection in sorted(reader.connections, key=lambda item: item.topic)
        ],
    }


def export_compressed_image(
    msg: object,
    seq_idx: int,
    bag_timestamp_ns: int,
    topic_root: Path,
    rows: list[dict[str, object]],
) -> None:
    data_dir = topic_root / "data"
    ensure_dir(data_dir)
    header_stamp_ns = message_header_stamp_ns(msg)
    image_format = str(getattr(msg, "format", "")).lower()
    suffix = "jpg" if image_format.startswith("jpeg") else ("png" if image_format.startswith("png") else "bin")
    filename = f"{seq_idx:06d}_{header_stamp_ns or bag_timestamp_ns}.{suffix}"
    raw = bytes(msg.data)
    (data_dir / filename).write_bytes(raw)
    rows.append(
        {
            "seq_idx": seq_idx,
            "bag_timestamp_ns": bag_timestamp_ns,
            "header_stamp_ns": header_stamp_ns,
            "file": f"data/{filename}",
            "format": msg.format,
            "size_bytes": len(raw),
        }
    )


def export_lidar_frame(
    msg: object,
    seq_idx: int,
    bag_timestamp_ns: int,
    topic_root: Path,
    rows: list[dict[str, object]],
) -> None:
    data_dir = topic_root / "data"
    ensure_dir(data_dir)
    header_stamp_ns = message_header_stamp_ns(msg)
    filename = f"{seq_idx:06d}_{header_stamp_ns or bag_timestamp_ns}.npz"

    point_num = int(msg.point_num)
    points = np.empty(
        point_num,
        dtype=[
            ("offset_time", np.uint32),
            ("x", np.float32),
            ("y", np.float32),
            ("z", np.float32),
            ("reflectivity", np.uint8),
            ("tag", np.uint8),
            ("line", np.uint8),
        ],
    )

    for i, pt in enumerate(msg.points):
        points[i] = (
            int(pt.offset_time),
            float(pt.x),
            float(pt.y),
            float(pt.z),
            int(pt.reflectivity),
            int(pt.tag),
            int(pt.line),
        )

    np.savez_compressed(
        data_dir / filename,
        points=points,
        header_stamp_ns=-1 if header_stamp_ns is None else header_stamp_ns,
        bag_timestamp_ns=bag_timestamp_ns,
        timebase_ns=int(msg.timebase),
        lidar_id=int(msg.lidar_id),
    )
    rows.append(
        {
            "seq_idx": seq_idx,
            "bag_timestamp_ns": bag_timestamp_ns,
            "header_stamp_ns": header_stamp_ns,
            "timebase_ns": int(msg.timebase),
            "point_num": point_num,
            "file": f"data/{filename}",
        }
    )


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False))
        f.write("\n")


def build_topic_manifest(topic: str, msgtype: str, root: Path) -> dict[str, Any]:
    return {
        "topic": topic,
        "msgtype": msgtype,
        "root": str(root),
        "message_count": 0,
        "header_stamp_count": 0,
        "bag_timestamp_min_ns": None,
        "bag_timestamp_max_ns": None,
        "header_timestamp_min_ns": None,
        "header_timestamp_max_ns": None,
        "artifacts": [],
    }


def choose_alignment_anchor(topic_states: dict[str, dict[str, Any]], requested_anchor: str | None) -> str:
    if requested_anchor:
        if requested_anchor not in topic_states:
            available = ", ".join(sorted(topic_states))
            raise ValueError(f"Alignment anchor topic not found: {requested_anchor}. Available topics: {available}")
        return requested_anchor

    for topic in DEFAULT_ALIGNMENT_ANCHORS:
        if topic in topic_states:
            return topic

    if not topic_states:
        raise ValueError("No topics available for alignment.")

    return sorted(topic_states)[0]


def nearest_row(rows: list[dict[str, Any]], timestamps: list[int], target_ns: int) -> dict[str, Any] | None:
    if not rows:
        return None

    pos = bisect.bisect_left(timestamps, target_ns)
    candidates: list[tuple[int, dict[str, Any]]] = []
    if pos < len(rows):
        candidates.append((abs(timestamps[pos] - target_ns), rows[pos]))
    if pos > 0:
        candidates.append((abs(timestamps[pos - 1] - target_ns), rows[pos - 1]))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]["seq_idx"]))
    return candidates[0][1]


def window_rows(
    rows: list[dict[str, Any]],
    timestamps: list[int],
    start_ns: int,
    end_ns: int | None,
) -> list[dict[str, Any]]:
    left = bisect.bisect_left(timestamps, start_ns)
    right = len(rows) if end_ns is None else bisect.bisect_left(timestamps, end_ns)
    return rows[left:right]


def build_alignment_rows(
    topic_states: dict[str, dict[str, Any]],
    anchor_topic: str,
    alignment_topics: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    anchor_state = topic_states[anchor_topic]
    anchor_rows = sorted(anchor_state["timeline_rows"], key=lambda item: (item["time_ns"], item["seq_idx"]))
    if not anchor_rows:
        return [], []

    sorted_topic_rows: dict[str, list[dict[str, Any]]] = {}
    sorted_topic_times: dict[str, list[int]] = {}
    for topic in alignment_topics:
        rows = sorted(topic_states[topic]["timeline_rows"], key=lambda item: (item["time_ns"], item["seq_idx"]))
        sorted_topic_rows[topic] = rows
        sorted_topic_times[topic] = [int(row["time_ns"]) for row in rows]

    fieldnames = [
        "anchor_topic",
        "anchor_msgtype",
        "anchor_seq_idx",
        "anchor_time_ns",
        "anchor_time_source",
        "anchor_bag_timestamp_ns",
        "anchor_header_stamp_ns",
        "anchor_next_time_ns",
        "anchor_artifact",
    ]

    rows_out: list[dict[str, Any]] = []
    for idx, anchor in enumerate(anchor_rows):
        anchor_time_ns = int(anchor["time_ns"])
        next_anchor_time_ns = int(anchor_rows[idx + 1]["time_ns"]) if idx + 1 < len(anchor_rows) else None
        row_out: dict[str, Any] = {
            "anchor_topic": anchor_topic,
            "anchor_msgtype": anchor["msgtype"],
            "anchor_seq_idx": anchor["seq_idx"],
            "anchor_time_ns": anchor_time_ns,
            "anchor_time_source": anchor["time_source"],
            "anchor_bag_timestamp_ns": anchor["bag_timestamp_ns"],
            "anchor_header_stamp_ns": anchor["header_stamp_ns"],
            "anchor_next_time_ns": next_anchor_time_ns,
            "anchor_artifact": anchor.get("artifact"),
        }

        for topic in alignment_topics:
            key = topic_key(topic)
            nearest = nearest_row(sorted_topic_rows[topic], sorted_topic_times[topic], anchor_time_ns)
            prefix = f"{key}__"

            nearest_seq_field = f"{prefix}nearest_seq_idx"
            nearest_time_field = f"{prefix}nearest_time_ns"
            nearest_source_field = f"{prefix}nearest_time_source"
            nearest_delta_field = f"{prefix}nearest_delta_ns"
            nearest_artifact_field = f"{prefix}nearest_artifact"

            for name in [
                nearest_seq_field,
                nearest_time_field,
                nearest_source_field,
                nearest_delta_field,
                nearest_artifact_field,
            ]:
                if name not in fieldnames:
                    fieldnames.append(name)

            if nearest is None:
                row_out[nearest_seq_field] = None
                row_out[nearest_time_field] = None
                row_out[nearest_source_field] = None
                row_out[nearest_delta_field] = None
                row_out[nearest_artifact_field] = None
            else:
                row_out[nearest_seq_field] = nearest["seq_idx"]
                row_out[nearest_time_field] = nearest["time_ns"]
                row_out[nearest_source_field] = nearest["time_source"]
                row_out[nearest_delta_field] = int(nearest["time_ns"]) - anchor_time_ns
                row_out[nearest_artifact_field] = nearest.get("artifact")

            if topic_states[topic]["msgtype"] == IMU_MSG:
                imu_window = window_rows(sorted_topic_rows[topic], sorted_topic_times[topic], anchor_time_ns, next_anchor_time_ns)
                imu_fields = [
                    f"{prefix}window_count",
                    f"{prefix}window_first_seq_idx",
                    f"{prefix}window_last_seq_idx",
                    f"{prefix}window_first_time_ns",
                    f"{prefix}window_last_time_ns",
                ]
                for name in imu_fields:
                    if name not in fieldnames:
                        fieldnames.append(name)

                row_out[f"{prefix}window_count"] = len(imu_window)
                row_out[f"{prefix}window_first_seq_idx"] = None if not imu_window else imu_window[0]["seq_idx"]
                row_out[f"{prefix}window_last_seq_idx"] = None if not imu_window else imu_window[-1]["seq_idx"]
                row_out[f"{prefix}window_first_time_ns"] = None if not imu_window else imu_window[0]["time_ns"]
                row_out[f"{prefix}window_last_time_ns"] = None if not imu_window else imu_window[-1]["time_ns"]

        rows_out.append(row_out)

    return rows_out, fieldnames


def write_alignment_outputs(
    output_dir: Path,
    topic_states: dict[str, dict[str, Any]],
    requested_anchor: str | None,
) -> dict[str, Any]:
    anchor_topic = choose_alignment_anchor(topic_states, requested_anchor)
    alignment_topics = [topic for topic in sorted(topic_states) if topic != anchor_topic]

    rows_out, fieldnames = build_alignment_rows(topic_states, anchor_topic, alignment_topics)
    alignment_dir = output_dir / "alignment"
    ensure_dir(alignment_dir)

    key = topic_key(anchor_topic)
    csv_path = alignment_dir / f"{key}_aligned.csv"
    manifest_path = alignment_dir / f"{key}_aligned_manifest.json"

    if rows_out:
        write_csv(csv_path, rows_out, fieldnames)

    manifest = {
        "anchor_topic": anchor_topic,
        "anchor_msgtype": topic_states[anchor_topic]["msgtype"],
        "row_count": len(rows_out),
        "time_basis": "header preferred, fallback to bag timestamp",
        "aligned_topics": alignment_topics,
        "csv": str(csv_path),
    }
    write_json(manifest_path, manifest)
    return manifest


def update_manifest_timestamps(manifest: dict[str, Any], bag_timestamp_ns: int, header_stamp_ns: int | None) -> None:
    manifest["message_count"] += 1
    manifest["bag_timestamp_min_ns"] = (
        bag_timestamp_ns
        if manifest["bag_timestamp_min_ns"] is None
        else min(manifest["bag_timestamp_min_ns"], bag_timestamp_ns)
    )
    manifest["bag_timestamp_max_ns"] = (
        bag_timestamp_ns
        if manifest["bag_timestamp_max_ns"] is None
        else max(manifest["bag_timestamp_max_ns"], bag_timestamp_ns)
    )
    if header_stamp_ns is None:
        return
    manifest["header_stamp_count"] += 1
    manifest["header_timestamp_min_ns"] = (
        header_stamp_ns
        if manifest["header_timestamp_min_ns"] is None
        else min(manifest["header_timestamp_min_ns"], header_stamp_ns)
    )
    manifest["header_timestamp_max_ns"] = (
        header_stamp_ns
        if manifest["header_timestamp_max_ns"] is None
        else max(manifest["header_timestamp_max_ns"], header_stamp_ns)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract all topic data from a ROS bag without requiring a full ROS installation."
    )
    parser.add_argument("bag", type=Path, help="Path to a ROS1/ROS2 bag file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory. Default: <bag_stem>_extracted next to the bag.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only print topic summary and exit.",
    )
    parser.add_argument(
        "--save-serialized",
        action="store_true",
        help="Also save each ROS-serialized message as a .bin file.",
    )
    parser.add_argument(
        "--align",
        action="store_true",
        help="Generate a same-timeline alignment CSV after export.",
    )
    parser.add_argument(
        "--align-anchor",
        default=None,
        help="Anchor topic used as the main time axis. Default prefers /livox/lidar.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete the existing output directory before exporting.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bag = args.bag.expanduser().resolve()
    if not bag.exists():
        raise FileNotFoundError(f"Bag file not found: {bag}")

    output_dir = args.output.expanduser().resolve() if args.output else bag.with_name(f"{bag.stem}_extracted")
    if output_dir.exists():
        if args.overwrite:
            shutil.rmtree(output_dir)
        elif any(output_dir.iterdir()):
            raise FileExistsError(
                f"Output directory already exists and is not empty: {output_dir}. "
                "Use --overwrite or choose a new --output path."
            )

    with AnyReader([bag]) as reader:
        summary = build_summary(reader, bag)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        if args.list_only:
            return 0

        ensure_dir(output_dir)
        topics_root = output_dir / "topics"
        serialized_root = output_dir / "serialized"
        ensure_dir(topics_root)
        if args.save_serialized:
            ensure_dir(serialized_root)

        write_json(output_dir / "summary.json", summary)

        topic_states: dict[str, dict[str, Any]] = {}

        for connection, bag_timestamp_ns, rawdata in reader.messages():
            topic = connection.topic
            msgtype = connection.msgtype
            msg = reader.deserialize(rawdata, msgtype)
            header_stamp_ns = message_header_stamp_ns(msg)

            if topic not in topic_states:
                root = topic_dir(topics_root, topic)
                ensure_dir(root)
                topic_states[topic] = {
                    "msgtype": msgtype,
                    "root": root,
                    "manifest": build_topic_manifest(topic, msgtype, root),
                    "seq_idx": 0,
                    "image_rows": [],
                    "lidar_rows": [],
                    "imu_rows": [],
                    "navsat_rows": [],
                    "timeline_rows": [],
                    "jsonl_path": root / "messages.jsonl",
                }

            state = topic_states[topic]
            seq_idx = int(state["seq_idx"])
            state["seq_idx"] = seq_idx + 1
            time_ns = effective_timestamp_ns(int(bag_timestamp_ns), header_stamp_ns)
            time_source = effective_timestamp_source(header_stamp_ns)

            update_manifest_timestamps(state["manifest"], int(bag_timestamp_ns), header_stamp_ns)

            if args.save_serialized:
                serial_dir = topic_dir(serialized_root, topic)
                ensure_dir(serial_dir)
                serial_name = f"{seq_idx:06d}_{int(bag_timestamp_ns)}.bin"
                (serial_dir / serial_name).write_bytes(rawdata)
                if "serialized/*.bin" not in state["manifest"]["artifacts"]:
                    state["manifest"]["artifacts"].append("serialized/*.bin")

            if msgtype == COMPRESSED_IMAGE:
                export_compressed_image(msg, seq_idx, int(bag_timestamp_ns), state["root"], state["image_rows"])
                state["timeline_rows"].append(
                    {
                        "seq_idx": seq_idx,
                        "bag_timestamp_ns": int(bag_timestamp_ns),
                        "header_stamp_ns": header_stamp_ns,
                        "time_ns": time_ns,
                        "time_source": time_source,
                        "artifact": f"data/{seq_idx:06d}_{header_stamp_ns or int(bag_timestamp_ns)}."
                        + ("jpg" if str(getattr(msg, "format", "")).lower().startswith("jpeg") else ("png" if str(getattr(msg, "format", "")).lower().startswith("png") else "bin")),
                        "msgtype": msgtype,
                    }
                )
                if "data/*.(jpg|png|bin)" not in state["manifest"]["artifacts"]:
                    state["manifest"]["artifacts"].extend(["data/*.(jpg|png|bin)", "index.csv"])
                continue

            if msgtype == LIVOX_CUSTOM_MSG:
                export_lidar_frame(msg, seq_idx, int(bag_timestamp_ns), state["root"], state["lidar_rows"])
                state["timeline_rows"].append(
                    {
                        "seq_idx": seq_idx,
                        "bag_timestamp_ns": int(bag_timestamp_ns),
                        "header_stamp_ns": header_stamp_ns,
                        "time_ns": time_ns,
                        "time_source": time_source,
                        "artifact": f"data/{seq_idx:06d}_{header_stamp_ns or int(bag_timestamp_ns)}.npz",
                        "msgtype": msgtype,
                    }
                )
                if "data/*.npz" not in state["manifest"]["artifacts"]:
                    state["manifest"]["artifacts"].extend(["data/*.npz", "index.csv"])
                continue

            if msgtype == IMU_MSG:
                state["imu_rows"].append(
                    {
                        "seq_idx": seq_idx,
                        "bag_timestamp_ns": int(bag_timestamp_ns),
                        "header_stamp_ns": header_stamp_ns,
                        "ang_vel_x": float(msg.angular_velocity.x),
                        "ang_vel_y": float(msg.angular_velocity.y),
                        "ang_vel_z": float(msg.angular_velocity.z),
                        "lin_acc_x": float(msg.linear_acceleration.x),
                        "lin_acc_y": float(msg.linear_acceleration.y),
                        "lin_acc_z": float(msg.linear_acceleration.z),
                        "orientation_x": float(msg.orientation.x),
                        "orientation_y": float(msg.orientation.y),
                        "orientation_z": float(msg.orientation.z),
                        "orientation_w": float(msg.orientation.w),
                    }
                )
                state["timeline_rows"].append(
                    {
                        "seq_idx": seq_idx,
                        "bag_timestamp_ns": int(bag_timestamp_ns),
                        "header_stamp_ns": header_stamp_ns,
                        "time_ns": time_ns,
                        "time_source": time_source,
                        "artifact": "imu.csv",
                        "msgtype": msgtype,
                    }
                )
                if "imu.csv" not in state["manifest"]["artifacts"]:
                    state["manifest"]["artifacts"].append("imu.csv")
                continue

            if msgtype == NAVSATFIX_MSG:
                status = getattr(msg, "status", None)
                state["navsat_rows"].append(
                    {
                        "seq_idx": seq_idx,
                        "bag_timestamp_ns": int(bag_timestamp_ns),
                        "header_stamp_ns": header_stamp_ns,
                        "latitude": float(msg.latitude),
                        "longitude": float(msg.longitude),
                        "altitude": float(msg.altitude),
                        "status": None if status is None else int(getattr(status, "status", 0)),
                        "service": None if status is None else int(getattr(status, "service", 0)),
                        "position_covariance_type": int(msg.position_covariance_type),
                        "position_covariance": json.dumps(list(msg.position_covariance), ensure_ascii=False),
                    }
                )
                state["timeline_rows"].append(
                    {
                        "seq_idx": seq_idx,
                        "bag_timestamp_ns": int(bag_timestamp_ns),
                        "header_stamp_ns": header_stamp_ns,
                        "time_ns": time_ns,
                        "time_source": time_source,
                        "artifact": "navsatfix.csv",
                        "msgtype": msgtype,
                    }
                )
                if "navsatfix.csv" not in state["manifest"]["artifacts"]:
                    state["manifest"]["artifacts"].append("navsatfix.csv")
                continue

            record = {
                "seq_idx": seq_idx,
                "bag_timestamp_ns": int(bag_timestamp_ns),
                "header_stamp_ns": header_stamp_ns,
                "topic": topic,
                "msgtype": msgtype,
                "message": to_plain_object(msg),
            }
            append_jsonl(state["jsonl_path"], record)
            state["timeline_rows"].append(
                {
                    "seq_idx": seq_idx,
                    "bag_timestamp_ns": int(bag_timestamp_ns),
                    "header_stamp_ns": header_stamp_ns,
                    "time_ns": time_ns,
                    "time_source": time_source,
                    "artifact": "messages.jsonl",
                    "msgtype": msgtype,
                }
            )
            if "messages.jsonl" not in state["manifest"]["artifacts"]:
                state["manifest"]["artifacts"].append("messages.jsonl")

        per_topic_stats: list[dict[str, Any]] = []
        for topic, state in sorted(topic_states.items(), key=lambda item: item[0]):
            root = Path(state["root"])
            if state["image_rows"]:
                write_csv(
                    root / "index.csv",
                    state["image_rows"],
                    ["seq_idx", "bag_timestamp_ns", "header_stamp_ns", "file", "format", "size_bytes"],
                )
            if state["lidar_rows"]:
                write_csv(
                    root / "index.csv",
                    state["lidar_rows"],
                    ["seq_idx", "bag_timestamp_ns", "header_stamp_ns", "timebase_ns", "point_num", "file"],
                )
            if state["imu_rows"]:
                write_csv(
                    root / "imu.csv",
                    state["imu_rows"],
                    [
                        "seq_idx",
                        "bag_timestamp_ns",
                        "header_stamp_ns",
                        "ang_vel_x",
                        "ang_vel_y",
                        "ang_vel_z",
                        "lin_acc_x",
                        "lin_acc_y",
                        "lin_acc_z",
                        "orientation_x",
                        "orientation_y",
                        "orientation_z",
                        "orientation_w",
                    ],
                )
            if state["navsat_rows"]:
                write_csv(
                    root / "navsatfix.csv",
                    state["navsat_rows"],
                    [
                        "seq_idx",
                        "bag_timestamp_ns",
                        "header_stamp_ns",
                        "latitude",
                        "longitude",
                        "altitude",
                        "status",
                        "service",
                        "position_covariance_type",
                        "position_covariance",
                    ],
                )

            manifest = state["manifest"]
            manifest["artifacts"] = sorted(set(manifest["artifacts"]))
            write_json(root / "manifest.json", manifest)

            per_topic_stats.append(
                {
                    "topic": topic,
                    "msgtype": state["msgtype"],
                    "message_count": manifest["message_count"],
                    "header_stamp_count": manifest["header_stamp_count"],
                    "root": str(root),
                    "artifacts": manifest["artifacts"],
                }
            )

        extract_stats = {
            "topic_count": len(topic_states),
            "message_count": sum(int(item["message_count"]) for item in per_topic_stats),
            "per_topic": per_topic_stats,
        }
        if args.align:
            extract_stats["alignment"] = write_alignment_outputs(output_dir, topic_states, args.align_anchor)
        write_json(output_dir / "extract_stats.json", extract_stats)
        print(json.dumps(extract_stats, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
