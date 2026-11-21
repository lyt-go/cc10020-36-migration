"""消息队列消费领域服务：消费组管理、偏移追踪、消息拉取与死信投递。

提供消息消费的完整生命周期管理，包括消费组注册、批量拉取、偏移提交、
超时处理以及失败消息的死信队列路由。
跨模块关联：dlq_handler（死信路由）、call_trace_log（消费追踪）。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class MqConsumerService(BaseModuleService):
    MODULE = "mq_consumer"
    ENTITY = "消费配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["配置编号", "队列名称", "消费组名", "偏移策略", "批量大小", "超时时间", "死信队列", "消费状态"]
    REQUIRED_FIELDS = ["配置编号", "队列名称", "消费组名"]
    STATUS_ORDER = ["草稿", "待校验", "消费中", "已停止"]
    ACTION_RULES = {"提交校验": "待校验", "启动消费": "消费中", "停止消费": "已停止"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._offsets: dict[str, int] = {}
        self._consumed_messages: list[dict[str, Any]] = []

    def register_consumer(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """注册新的消费者配置。"""
        return self.create_entry(values)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建消费配置，附带初始偏移和消费组注册。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)
        entry = {"id": max_id + 1}
        entry.update({field: values.get(field, "") for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["created_at"] = datetime.now(timezone.utc).isoformat()
        entry["current_offset"] = 0
        entry["messages_consumed"] = 0
        entry["last_commit_at"] = None

        json_store.insert(self.MODULE, entry)

        # 初始化偏移
        queue_name = str(values.get("队列名称", ""))
        group_name = str(values.get("消费组名", ""))
        offset_key = f"{queue_name}:{group_name}"
        self._offsets[offset_key] = 0

        return entry, []

    def consume_batch(self, consumer_id: int, max_messages: int = 10) -> dict[str, Any]:
        """从模拟消息队列中批量消费消息。

        流程：
        1. 检查消费者状态是否为"消费中"
        2. 基于当前偏移生成模拟消息
        3. 逐条消费并校验
        4. 消费失败的消息路由到死信队列
        5. 提交偏移

        返回消费结果包含成功/失败消息明细。
        """
        consumer = json_store.get_by_id(self.MODULE, consumer_id)
        if consumer is None:
            return {"ok": False, "message": f"消费者 {consumer_id} 不存在", "consumed": 0, "failed": 0}

        if consumer.get("status") != "消费中":
            return {"ok": False, "message": f"消费者未启动（当前状态: {consumer['status']}）", "consumed": 0, "failed": 0}

        queue_name = str(consumer.get("队列名称", "default"))
        group_name = str(consumer.get("消费组名", "default"))
        offset_key = f"{queue_name}:{group_name}"
        batch_size = int(consumer.get("批量大小", str(max_messages)))

        current_offset = self._offsets.get(offset_key, int(consumer.get("current_offset", 0)))
        timeout_ms = int(str(consumer.get("超时时间", "5000") or "5000").split("T")[0] if "T" in str(consumer.get("超时时间", "")) else str(consumer.get("超时时间", "5000") or "5000"))

        # 模拟消息生成
        messages = []
        for i in range(min(batch_size, max_messages)):
            messages.append({
                "offset": current_offset + i,
                "message_id": str(uuid.uuid4()),
                "queue": queue_name,
                "group": group_name,
                "payload": {
                    "type": "data_event",
                    "data": {
                        "id": current_offset + i + 1,
                        "value": f"record_{current_offset + i + 1}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        consumed = 0
        failed = 0
        dlq_records = []

        import random
        for msg in messages:
            # 模拟处理：20%概率失败
            if random.random() < 0.2:
                failed += 1
                # 路由到死信队列（跨模块：dlq_handler）
                dlq_record = self._route_to_dlq(consumer_id, msg, "处理超时" if consumed % 3 == 0 else "数据格式异常")
                dlq_records.append(dlq_record)
            else:
                consumed += 1
                # 记录消费追踪（跨模块：call_trace_log）
                self._record_consumption(consumer_id, msg, timeout_ms)

        # 提交偏移
        if consumed + failed > 0:
            new_offset = current_offset + consumed + failed
            self._offsets[offset_key] = new_offset
            json_store.update(self.MODULE, consumer_id, {
                "current_offset": new_offset,
                "messages_consumed": int(consumer.get("messages_consumed", 0)) + consumed + failed,
                "last_commit_at": datetime.now(timezone.utc).isoformat(),
            })

        return {
            "ok": True,
            "message": f"消费完成: 成功 {consumed}, 失败 {failed}",
            "consumed": consumed,
            "failed": failed,
            "total": len(messages),
            "dlq_records": len(dlq_records),
            "previous_offset": current_offset,
            "new_offset": self._offsets.get(offset_key, current_offset),
        }

    def _route_to_dlq(self, consumer_id: int, message: dict[str, Any], reason: str) -> dict[str, Any]:
        """将消费失败的消息路由到死信队列。

        消息体以结构化载荷 + 显式版本入队，原文（JSON 文本）同时保留，
        供旧消费者按原样定位消息。
        """
        try:
            from app.services.dlq_handler import DlqHandlerService
            dlq_svc = DlqHandlerService()
            payload = message.get("payload", {})
            return dlq_svc.enqueue_dead_letter(
                source_queue=str(message.get("queue", "unknown")),
                original_message=None,
                original_payload=payload,
                payload_type="structured",
                raw_text=json.dumps(payload, ensure_ascii=False),
                failure_reason=reason,
                consumer_id=consumer_id,
            )
        except Exception as e:
            return {"ok": False, "reason": str(e)}

    # ------------------------------------------------------------------
    # 跨模块重放：从 dlq_handler 取得版本化载荷，按所用版本执行消费
    # ------------------------------------------------------------------
    def consume_replayed_dead_letter(
        self,
        dlq_id: int,
        *,
        deployment_version: str | None = None,
        batch_id: str = "",
    ) -> dict[str, Any]:
        """消费一条待重放的死信载荷。

        内容来源是 dlq_handler 的版本化死信载荷（可能与旧字符串副本同时
        存在）。本方法按载荷声明的版本解释内容，并把消费结果写回死信复核
        清单；同一个死信编号的并发重放只会确认一份实际结果。
        """
        from app.services.dlq_handler import DlqHandlerService
        from app.services.dlq_payload import DEPLOYMENT_VERSION, PayloadVersionError

        version = deployment_version or DEPLOYMENT_VERSION
        dlq_svc = DlqHandlerService()
        try:
            view = dlq_svc.get_payload_for_replay(dlq_id, deployment_version=version)
        except PayloadVersionError as exc:
            return {
                "ok": False,
                "dlq_id": dlq_id,
                "message": str(exc),
                "payload_version": getattr(exc, "version", None),
            }

        if not view.get("ok"):
            return {
                "ok": False,
                "dlq_id": dlq_id,
                "message": view.get("message", "载荷不可用"),
                "payload_version": view.get("payload_version"),
            }

        try:
            outcome = self._consume_by_version(view, deployment_version=version)
        except PayloadVersionError as exc:
            return {
                "ok": False,
                "dlq_id": dlq_id,
                "message": str(exc),
                "payload_version": getattr(exc, "version", None),
                "consumer_version": version,
            }
        if not outcome["ok"]:
            return {
                "ok": False,
                "dlq_id": dlq_id,
                "message": outcome["message"],
                "payload_version": outcome.get("payload_version"),
                "consumer_version": version,
            }

        actual_id = f"CONSUME-{view.get('记录编号') or dlq_id}-{uuid.uuid4().hex[:8].upper()}"
        consumption = {
            "ok": True,
            "actual_consumption_id": actual_id,
            "dlq_id": dlq_id,
            "payload_version": outcome["payload_version"],
            "payload_type": outcome["payload_type"],
            "received_payload": outcome["received_payload"],
            "source": view.get("source"),
            "consumer_version": version,
            "batch_id": batch_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._consumed_messages.append(consumption)
        # 跨模块写回：消费结果直接落死信复核清单。
        dlq_svc.record_review_entry(
            dlq_id,
            payload_view=view,
            consumption=consumption,
            batch_id=batch_id,
            deployment_version=version,
        )

        return {
            "ok": True,
            "dlq_id": dlq_id,
            "actual_consumption_id": actual_id,
            "consumer_version": version,
            "payload_version": outcome["payload_version"],
            "payload_type": outcome["payload_type"],
            "received_payload": outcome["received_payload"],
            "raw_text": view.get("raw_text"),
            "message": f"按载荷版本 {outcome['payload_version']} 完成消费",
            "review": self._latest_review_for(dlq_id),
            "payload_view": {
                **view,
                "payload_version": outcome["payload_version"],
                "payload_type": outcome["payload_type"],
            },
        }

    def _latest_review_for(self, dlq_id: int) -> dict[str, Any] | None:
        from app.services.dlq_handler import DlqHandlerService

        for row in reversed(DlqHandlerService().list_review_ledger(dlq_id)):
            if row.get("result_status") == "已确认":
                return row
        return None

    def _consume_by_version(
        self,
        view: dict[str, Any],
        *,
        deployment_version: str,
    ) -> dict[str, Any]:
        """按声明的载荷版本解释并执行消费；版本不明时不猜测。"""
        from app.services.dlq_payload import (
            SUPPORTED_PAYLOAD_VERSIONS,
            PayloadVersionError,
            interpret_for_consumption,
        )

        version = view.get("payload_version")
        try:
            version_number = int(version)
        except (TypeError, ValueError):
            raise PayloadVersionError(version, f"死信载荷版本无效：{version!r}")
        if version_number not in SUPPORTED_PAYLOAD_VERSIONS:
            raise PayloadVersionError(
                version,
                f"消费者不支持死信载荷版本 {version}（支持 {SUPPORTED_PAYLOAD_VERSIONS}）",
            )

        interpreted = interpret_for_consumption(view["payload"])
        received = interpreted["received_payload"]
        if interpreted["payload_type"] == "structured":
            if not isinstance(received, (dict, list)):
                return {
                    "ok": False,
                    "message": f"版本 {version_number} 声明为结构化载荷，但内容不是对象",
                    "payload_version": version_number,
                }
            # 结构化消息按声明的版本解释；v2 起以 data 字段承载业务数据。
            if version_number >= 2 and isinstance(received, dict):
                business = received.get("data", received)
            else:
                business = received
            return {
                "ok": True,
                "payload_version": version_number,
                "payload_type": "structured",
                "received_payload": business,
                "message": f"结构化载荷已按版本 {version_number} 消费",
            }

        # 文本载荷：按原文逐字消费，形似 JSON 也不解析。
        return {
            "ok": True,
            "payload_version": version_number,
            "payload_type": "text",
            "received_payload": received,
            "message": f"文本载荷已按版本 {version_number} 原样消费",
        }

    def _record_consumption(self, consumer_id: int, message: dict[str, Any], latency_ms: int) -> None:
        """记录消费成功的追踪信息。"""
        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            tracer._record_trace(
                endpoint_id=consumer_id,
                action="消息消费",
                latency_ms=min(latency_ms, 5000),
                status="正常",
                record_count=1,
            )
        except Exception:
            pass

    def get_consumer_state(self, consumer_id: int) -> dict[str, Any]:
        """获取消费者状态，包含偏移和健康度。"""
        consumer = json_store.get_by_id(self.MODULE, consumer_id)
        if consumer is None:
            return {"ok": False, "message": f"消费者 {consumer_id} 不存在"}

        queue_name = str(consumer.get("队列名称", ""))
        group_name = str(consumer.get("消费组名", ""))
        offset_key = f"{queue_name}:{group_name}"

        return {
            "ok": True,
            "consumer_id": consumer_id,
            "queue": queue_name,
            "group": group_name,
            "status": consumer.get("status"),
            "current_offset": self._offsets.get(offset_key, consumer.get("current_offset", 0)),
            "messages_consumed": consumer.get("messages_consumed", 0),
            "last_commit_at": consumer.get("last_commit_at"),
            "is_healthy": consumer.get("status") == "消费中" and not consumer.get("abnormal"),
        }

    def reset_offset(self, consumer_id: int, target_offset: int) -> dict[str, Any]:
        """重置消费者偏移到指定位置（支持回溯和跳转）。"""
        consumer = json_store.get_by_id(self.MODULE, consumer_id)
        if consumer is None:
            return {"ok": False, "message": f"消费者 {consumer_id} 不存在"}

        if consumer.get("status") != "已停止":
            return {"ok": False, "message": "请先停止消费者再重置偏移"}

        queue_name = str(consumer.get("队列名称", ""))
        group_name = str(consumer.get("消费组名", ""))
        offset_key = f"{queue_name}:{group_name}"
        old_offset = self._offsets.get(offset_key, 0)
        self._offsets[offset_key] = target_offset

        json_store.update(self.MODULE, consumer_id, {"current_offset": target_offset})

        return {
            "ok": True,
            "message": f"偏移已从 {old_offset} 重置为 {target_offset}",
            "old_offset": old_offset,
            "new_offset": target_offset,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出消息队列消费的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "消息队列消费数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_messages_consumed": sum(int(r.get("messages_consumed", 0)) for r in rows),
            "active_consumers": sum(1 for r in rows if r.get("status") == "消费中"),
            "operation": self.OPERATION_TERM,
        }