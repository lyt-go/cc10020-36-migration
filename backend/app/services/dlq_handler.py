"""死信队列管理领域服务：失败消息隔离、原因分类分析、批量重放与处置策略。

升级后的关键约定：

* 新入队的消息体一律保存为带显式版本的 ``死信载荷`` 对象（见
  :mod:`app.services.dlq_payload`），不再把字符串直接当作唯一表示；
* 存量记录通过可中断、可续做的迁移补齐版本化副本，迁移过程不改动
  死信编号、原始队列、记录编号等消息身份字段；
* 每次入队生成一份稳定的“处置凭证”（receipt），迁移、回滚、换部署
  版本都不会丢失，重放确认后会写入唯一的实际消费编号；
* 重放按载荷声明的版本解释内容，结果写入死信复核清单（review ledger）；
* 同一死信编号的并发重放只确认一份实际消费结果。

跨模块关联：mq_consumer（按版本消费重放内容）、call_trace_log（重放追踪）、
data_quality_rule（质量校验）。
"""
from __future__ import annotations

import hashlib
import threading
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.store import store
from app.services.base import BaseModuleService
from app.services.dlq_payload import (
    DEPLOYMENT_VERSION,
    PAYLOAD_SCHEMA_VERSION,
    SUPPORTED_PAYLOAD_VERSIONS,
    PayloadVersionError,
    is_versioned_payload,
    legacy_text_payload,
    normalize_payload,
    structured_payload,
    text_payload,
)

# 迁移状态 / 回执 / 复核清单各自独立分区，避免污染死信记录列表。
MIGRATION_KEY = "dlq_migration_state"
RECEIPT_KEY = "dlq_receipt"
REVIEW_KEY = "dlq_review_ledger"

# 迁移可续做的状态集合。
_RESUMABLE_STATES = ("运行中", "已暂停", "已中断")
# 模块级锁：所有服务实例共享，保证并发重放只产生一份实际结果。
_REPLAY_LOCK = threading.RLock()
_MIGRATION_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha16(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _migration_message(state: dict[str, Any]) -> str:
    status = state.get("status")
    total = int(state.get("total", 0))
    cursor = int(state.get("cursor", 0))
    if status == "已中断":
        return f"迁移已中断，可在同一批次续做（{cursor}/{total} 已确认）"
    if status == "已完成":
        return f"迁移已完成（{cursor}/{total}）"
    return f"迁移{status}（{cursor}/{total} 已确认）"


class DlqHandlerService(BaseModuleService):
    MODULE = "dlq_handler"
    ENTITY = "死信记录"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["记录编号", "原始队列", "失败原因", "原始消息体", "重试次数", "入队时间", "处置状态", "重放批次"]
    REQUIRED_FIELDS = ["记录编号", "原始队列", "失败原因"]
    STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
    ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._replay_history: list[dict[str, Any]] = []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """列表合并持久化死信与示例数据，持久化记录优先。

        入队/迁移后的死信都落在持久化分区；只读示例仓库会看不到它们。
        """
        persisted = json_store.get_all(self.MODULE)
        seen = {int(row.get("id", 0)) for row in persisted}
        rows = list(persisted) + [
            row for row in store.rows(self.MODULE) if int(row.get("id", 0)) not in seen
        ]
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """按死信编号打开处置记录。

        优先持久化分区（真实入队/迁移过的记录），缺失时回退示例仓库；
        记录里附带处置凭证与复核清单，换部署版本后仍能按原编号对账。
        """
        entry = json_store.get_by_id(self.MODULE, entry_id)
        if entry is None:
            entry = super().get_entry(entry_id)
        if entry is None:
            return None
        result = dict(entry)
        receipt = self.get_receipt(entry_id)
        if receipt is not None:
            result["处置凭证"] = receipt
        result["复核清单"] = self.list_review_ledger(entry_id)
        return result

    # ------------------------------------------------------------------
    # 入队：始终写出带版本的载荷，并生成稳定处置凭证
    # ------------------------------------------------------------------
    def enqueue_dead_letter(
        self,
        source_queue: str,
        original_message: str | None = None,
        failure_reason: str = "",
        consumer_id: int = 0,
        *,
        original_payload: Any = None,
        payload_type: str | None = None,
        payload_version: int | None = None,
        raw_text: str | None = None,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any]:
        """将消费失败的消息加入死信队列。

        ``original_message`` 为普通字符串时按文本载荷保存，逐字保留，绝不
        做 JSON 解析——形似 JSON 的文本仍然是文本。真正的结构化消息由
        ``original_payload`` + ``payload_type="structured"``（或声明版本的
        载荷对象）传入，按声明的版本解释。
        """
        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)

        version = PAYLOAD_SCHEMA_VERSION if payload_version is None else int(payload_version)
        if version not in SUPPORTED_PAYLOAD_VERSIONS:
            raise PayloadVersionError(
                version,
                f"不支持的死信载荷版本：{version}（支持 {SUPPORTED_PAYLOAD_VERSIONS}）",
            )
        payload, display_text = self._build_payload(
            original_message=original_message,
            original_payload=original_payload,
            payload_type=payload_type,
            payload_version=version,
            raw_text=raw_text,
        )

        msg_fingerprint = _sha16(display_text)

        # 相同指纹的待处置死信合并，不重复生成凭证。
        duplicate = [
            e for e in existing
            if e.get("_fingerprint") == msg_fingerprint
            and e.get("status") in ("草稿", "待处置")
        ]
        if duplicate:
            existing_dlq = duplicate[0]
            retry_count = int(existing_dlq.get("重试次数", "0") or "0") + 1
            json_store.update(self.MODULE, int(existing_dlq["id"]), {
                "重试次数": str(retry_count),
                "updated_at": _now(),
            })
            receipt = self.get_receipt(int(existing_dlq["id"]), deployment_version=deployment_version)
            return {
                "ok": True,
                "message": f"消息已在死信队列中（retry#{retry_count}），合并为已有记录 #{existing_dlq['id']}",
                "dlq_id": existing_dlq["id"],
                "is_duplicate": True,
                "payload_version": payload.get("payload_version"),
                "receipt_id": receipt.get("receipt_id") if receipt else None,
            }

        record_no = (
            f"DLQ-{source_queue[:4].upper()}-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{max_id + 1:05d}"
        )

        dlq_entry = {
            "id": max_id + 1,
            "记录编号": record_no,
            "原始队列": source_queue,
            "失败原因": failure_reason,
            # 展示字段：保留原文，方便旧消费者/旧页面继续定位消息。
            "原始消息体": display_text,
            "重试次数": "0",
            "入队时间": _now(),
            "处置状态": "草稿",
            "重放批次": "",
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "created_at": _now(),
            # 版本化载荷：新消息的唯一权威表示。
            "死信载荷": payload,
            "载荷版本": payload["payload_version"],
            "载荷类型": payload["payload_type"],
            "迁移状态": "原生版本化",
            "消息身份": f"{source_queue}|{record_no}",
            "_fingerprint": msg_fingerprint,
            "_consumer_id": consumer_id,
            "_deployment_version": deployment_version,
        }

        for field in self.LIST_FIELDS:
            if field not in dlq_entry:
                dlq_entry[field] = ""

        json_store.insert(self.MODULE, dlq_entry)
        receipt = self._create_receipt(dlq_entry, payload, deployment_version=deployment_version)
        return {
            "ok": True,
            "message": f"死信已入队，编号 {record_no}",
            "dlq_id": dlq_entry["id"],
            "is_duplicate": False,
            "record_no": record_no,
            "payload_version": payload["payload_version"],
            "payload_type": payload["payload_type"],
            "receipt_id": receipt["receipt_id"],
            "message_identity": dlq_entry["消息身份"],
        }

    def _build_payload(
        self,
        *,
        original_message: Any,
        original_payload: Any,
        payload_type: str | None,
        payload_version: int,
        raw_text: str | None,
    ) -> tuple[dict[str, Any], str]:
        """决定入队载荷的版本化表示，返回 (载荷对象, 原文展示文本)。"""
        if original_payload is not None:
            if payload_type == "text" or (
                payload_type is None and isinstance(original_payload, str)
            ):
                text = str(original_payload)
                return (
                    text_payload(
                        text,
                        version=payload_version,
                        source="enqueue",
                        legacy_origin=False,
                    ),
                    text,
                )
            text = raw_text
            if text is None:
                text = original_message if isinstance(original_message, str) else ""
            return (
                structured_payload(
                    original_payload,
                    version=payload_version,
                    raw_text=text or None,
                    source="enqueue",
                    legacy_origin=False,
                ),
                text or "",
            )

        if isinstance(original_message, dict) and is_versioned_payload(original_message):
            normalized = normalize_payload(original_message)
            normalized["source"] = normalized.get("source") or "enqueue"
            return normalized, normalized["raw_text"]

        if isinstance(original_message, dict) or isinstance(original_message, list):
            # 调用方直接给了结构化对象，按当前版本保存。
            text = raw_text if raw_text is not None else original_message
            body_text = text if isinstance(text, str) else ""
            return (
                structured_payload(
                    original_message,
                    version=payload_version,
                    raw_text=body_text or None,
                    source="enqueue",
                    legacy_origin=False,
                ),
                body_text,
            )

        text = "" if original_message is None else str(original_message)
        if raw_text is not None:
            text = raw_text
        return (
            text_payload(
                text,
                version=payload_version,
                source="enqueue",
                legacy_origin=False,
            ),
            text,
        )

    # ------------------------------------------------------------------
    # 处置凭证：稳定、可跨部署版本定位
    # ------------------------------------------------------------------
    def _create_receipt(
        self,
        entry: dict[str, Any],
        payload: dict[str, Any],
        *,
        deployment_version: str,
        confirmed_at: str | None = None,
    ) -> dict[str, Any]:
        receipt = {
            "receipt_id": f"RCPT-{entry.get('记录编号', entry.get('id'))}",
            "dlq_id": int(entry["id"]),
            "记录编号": entry.get("记录编号", ""),
            "原始队列": entry.get("原始队列", ""),
            "消息身份": entry.get("消息身份", ""),
            "payload_version": payload.get("payload_version"),
            "payload_type": payload.get("payload_type"),
            "payload_digest": payload.get("digest"),
            "confirmed_at": confirmed_at or _now(),
            "receipt_status": "已确认",
            "deployment_version": deployment_version,
            "actual_consumption_id": None,
            "actual_consumption_payload_version": None,
            "actual_consumption_at": None,
            "replay_deployment_version": None,
        }
        json_store.insert(RECEIPT_KEY, receipt)
        return self.get_receipt(int(entry["id"])) or receipt

    def _ensure_receipt(
        self,
        entry: dict[str, Any],
        payload: dict[str, Any],
        *,
        deployment_version: str,
        confirmed_at: str | None = None,
    ) -> dict[str, Any]:
        """确保每条死信（含存量记录）都有可定位的处置凭证。"""
        existing = self.get_receipt(int(entry["id"]))
        if existing is not None:
            return existing
        return self._create_receipt(
            entry,
            payload,
            deployment_version=deployment_version,
            confirmed_at=confirmed_at,
        )

    def get_receipt(
        self,
        dlq_id: int,
        *,
        deployment_version: str | None = None,
    ) -> dict[str, Any] | None:
        """按死信编号读取处置凭证；与当前部署版本无关。"""
        for row in json_store.get_all(RECEIPT_KEY):
            if int(row.get("dlq_id", 0)) == int(dlq_id):
                result = dict(row)
                if deployment_version:
                    result["opened_by_deployment_version"] = deployment_version
                return result
        return None

    def find_receipt(self, record_no: str) -> dict[str, Any] | None:
        """按记录编号（消息身份）读取处置凭证。"""
        for row in json_store.get_all(RECEIPT_KEY):
            if row.get("记录编号") == record_no:
                return dict(row)
        return None

    def _update_receipt(self, dlq_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
        for row in json_store.get_all(RECEIPT_KEY):
            if int(row.get("dlq_id", 0)) == int(dlq_id):
                return json_store.update(RECEIPT_KEY, int(row["id"]), updates)
        return None

    # ------------------------------------------------------------------
    # 载荷解析：同一条记录可能同时存在旧字符串与版本化副本
    # ------------------------------------------------------------------
    def resolve_payload(
        self,
        dlq_id: int,
        *,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any] | None:
        """解析死信编号对应的可消费载荷，返回解析视图。"""
        entry = json_store.get_by_id(self.MODULE, dlq_id)
        if entry is None:
            return None
        try:
            return self._resolve_entry_payload(entry, deployment_version=deployment_version)
        except PayloadVersionError as exc:
            return {
                "dlq_id": int(entry.get("id", 0)),
                "记录编号": entry.get("记录编号", ""),
                "原始队列": entry.get("原始队列", ""),
                "payload": None,
                "payload_version": getattr(exc, "version", None),
                "payload_type": None,
                "raw_text": None,
                "digest": None,
                "source": "unresolved",
                "warning": str(exc),
                "deployment_version": deployment_version,
            }

    def get_payload_for_replay(
        self,
        dlq_id: int,
        *,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any]:
        """重放前解析载荷；供 mq_consumer 跨模块取用。"""
        view = self.resolve_payload(dlq_id, deployment_version=deployment_version)
        if view is None:
            return {
                "ok": False,
                "dlq_id": dlq_id,
                "message": f"死信记录 {dlq_id} 不存在或已归档",
                "deployment_version": deployment_version,
            }
        if view.get("payload") is None:
            view["ok"] = False
            view["message"] = view.get("warning") or "死信载荷版本不可用"
            return view
        view["ok"] = True
        return view

    def _resolve_entry_payload(
        self,
        entry: dict[str, Any],
        *,
        deployment_version: str,
    ) -> dict[str, Any]:
        legacy_text = entry.get("原始消息体")
        versioned = entry.get("死信载荷")
        payload: dict[str, Any] | None = None
        source = "legacy"
        warning = None

        if is_versioned_payload(versioned):
            try:
                normalized = normalize_payload(versioned)
            except PayloadVersionError as exc:
                warning = str(exc)
                normalized = None
                # 迁移副本版本异常（损坏/未完成）时可按原文兜底；
                # 原生版本化消息版本不受支持则明确拒绝，不猜测语义。
                legacy_origin = bool(
                    isinstance(versioned, dict)
                    and (
                        versioned.get("legacy_origin")
                        or versioned.get("source") == "migration"
                        or entry.get("迁移状态") in ("已写出", "已确认")
                    )
                )
                if not legacy_origin or not isinstance(legacy_text, str):
                    raise
            if normalized is not None:
                # 迁移副本必须与原文字段逐字一致；副本未确认、不完整或被改写时
                # 回退原文。升级后原生入队的版本化载荷不受旧字段约束。
                migrated = (
                    normalized.get("legacy_origin")
                    or normalized.get("source") == "migration"
                    or entry.get("迁移状态") in ("已写出", "已确认")
                )
                if migrated and isinstance(legacy_text, str):
                    consistent = normalized.get("payload_type") != "text" or (
                        normalized.get("raw_text") == legacy_text
                    )
                    if not consistent:
                        warning = "版本化副本与原文不一致，回退原文字段"
                        normalized = None
                if normalized is not None:
                    payload = normalized
                    source = "versioned"
        elif versioned is not None:
            warning = "死信载荷不是带显式版本的对象"

        if payload is None:
            if isinstance(legacy_text, str):
                # 存量字符串一律按原文解释；即使形似 JSON 也不解析。
                payload = text_payload(
                    legacy_text,
                    version=1,
                    source="legacy",
                    legacy_origin=True,
                )
                source = "legacy"
            elif isinstance(legacy_text, dict) and is_versioned_payload(legacy_text):
                payload = normalize_payload(legacy_text)
                source = "legacy_field"
            else:
                raise PayloadVersionError(
                    None,
                    f"死信记录 {entry.get('id')} 没有可解释的载荷（版本: {entry.get('载荷版本')}）",
                )

        return {
            "dlq_id": int(entry.get("id", 0)),
            "记录编号": entry.get("记录编号", ""),
            "原始队列": entry.get("原始队列", ""),
            "消息身份": entry.get("消息身份") or f"{entry.get('原始队列', '')}|{entry.get('记录编号', '')}",
            "payload": payload,
            "payload_version": payload["payload_version"],
            "payload_type": payload["payload_type"],
            "raw_text": payload["raw_text"],
            "digest": payload["digest"],
            "source": source,
            "has_legacy_copy": isinstance(legacy_text, str),
            "has_versioned_copy": is_versioned_payload(versioned),
            "migration_state": entry.get("迁移状态", ""),
            "warning": warning,
            "deployment_version": deployment_version,
        }

    # ------------------------------------------------------------------
    # 存量字符串载荷 → 版本化载荷迁移
    # ------------------------------------------------------------------
    def migrate_legacy_payloads(
        self,
        *,
        batch_id: str | None = None,
        resume: bool = False,
        limit: int | None = None,
        interrupt_after: int | None = None,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any]:
        """把存量字符串载荷迁移为版本化死信载荷。

        每一步先写出 ``死信载荷`` 副本（``迁移状态=已写出``），再确认
        （``迁移状态=已确认``）。``interrupt_after`` 可在确认前中断，用于
        验证“副本已写出、迁移未确认”的续做场景。迁移不修改 id、记录编号、
        原始队列等消息身份字段，也不删除 ``原始消息体``。
        """
        with _MIGRATION_LOCK:
            state = None
            if batch_id:
                state = self._find_migration(batch_id)
                if state is None:
                    return {"ok": False, "message": f"迁移批次 {batch_id} 不存在", "batch_id": batch_id}
                if state.get("status") == "已回滚":
                    return {
                        "ok": False,
                        "message": f"迁移批次 {batch_id} 已回滚，请重新发起迁移",
                        "batch_id": batch_id,
                        "status": "已回滚",
                    }
                if state.get("status") == "已完成":
                    return self._migration_result(state, resumed=False)
            elif resume:
                state = self._latest_migration(statuses=_RESUMABLE_STATES)
                if state is None:
                    return {"ok": False, "message": "没有可续做的迁移批次", "resumed": False}
                if state.get("status") == "已完成":
                    return self._migration_result(state, resumed=False)
            else:
                batch_id = f"MIG-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
                # 身份快照：迁移只处理发起时已存在的记录；期间新入队的消息
                # 已是原生版本化载荷，不进入本批次。
                targets = [int(r["id"]) for r in self._migration_snapshot()]
                state = json_store.insert(MIGRATION_KEY, {
                    "batch_id": batch_id,
                    "status": "运行中",
                    "target_ids": targets,
                    "cursor": 0,
                    "total": len(targets),
                    "migrated": 0,
                    "skipped": 0,
                    "failed": 0,
                    "interrupted": False,
                    "resumed": False,
                    "started_at": _now(),
                    "updated_at": _now(),
                    "deployment_version": deployment_version,
                })

            state_id = int(state["id"])
            resumed = bool(resume or state.get("resumed") or int(state.get("cursor", 0)) > 0)
            targets = list(state.get("target_ids") or [])
            cursor = int(state.get("cursor", 0))
            processed_this_run = 0

            state["status"] = "运行中"
            state["interrupted"] = False
            state["resumed"] = resumed
            json_store.update(MIGRATION_KEY, state_id, {
                "status": "运行中",
                "interrupted": False,
                "resumed": resumed,
                "updated_at": _now(),
            })

            while cursor < len(targets):
                if limit is not None and processed_this_run >= int(limit):
                    break
                dlq_id = int(targets[cursor])
                entry = json_store.get_by_id(self.MODULE, dlq_id)
                if entry is None:
                    cursor += 1
                    continue

                if self._is_confirmed(entry):
                    state["skipped"] = int(state.get("skipped", 0)) + 1
                    cursor += 1
                    continue

                legacy_text = entry.get("原始消息体")
                if not isinstance(legacy_text, str):
                    state["failed"] = int(state.get("failed", 0)) + 1
                    cursor += 1
                    continue

                payload = legacy_text_payload(
                    legacy_text,
                    migration_batch=state["batch_id"],
                )

                # 第一步：写出副本，身份字段保持原样；可在这里中断。
                json_store.update(self.MODULE, dlq_id, {
                    "死信载荷": payload,
                    "载荷版本": payload["payload_version"],
                    "载荷类型": payload["payload_type"],
                    "迁移状态": "已写出",
                    "迁移批次": state["batch_id"],
                    "迁移写入时间": _now(),
                })
                processed_this_run += 1

                if interrupt_after is not None and processed_this_run >= int(interrupt_after):
                    state["status"] = "已中断"
                    state["interrupted"] = True
                    state["cursor"] = cursor
                    state["migrated"] = int(state.get("migrated", 0))
                    state["updated_at"] = _now()
                    json_store.update(MIGRATION_KEY, state_id, {
                        "status": "已中断",
                        "interrupted": True,
                        "cursor": cursor,
                        "migrated": state["migrated"],
                        "skipped": state.get("skipped", 0),
                        "failed": state.get("failed", 0),
                        "updated_at": _now(),
                    })
                    state["id"] = state_id
                    return self._migration_result(state, resumed=resumed)

                # 第二步：确认副本与原文一致后落账。
                json_store.update(self.MODULE, dlq_id, {
                    "迁移状态": "已确认",
                    "迁移确认时间": _now(),
                })
                # 存量消息也在迁移时补上处置凭证，回滚后仍可按原编号定位。
                self._ensure_receipt(
                    entry,
                    payload,
                    deployment_version=deployment_version,
                    confirmed_at=entry.get("入队时间") or entry.get("created_at"),
                )
                state["migrated"] = int(state.get("migrated", 0)) + 1
                cursor += 1
                json_store.update(MIGRATION_KEY, state_id, {
                    "cursor": cursor,
                    "migrated": state["migrated"],
                    "skipped": state.get("skipped", 0),
                    "failed": state.get("failed", 0),
                    "updated_at": _now(),
                })

            state["cursor"] = cursor
            if cursor >= len(targets):
                state["status"] = "已完成"
                state["completed_at"] = _now()
                state["interrupted"] = False
            else:
                state["status"] = "已暂停"
            state["updated_at"] = _now()
            json_store.update(MIGRATION_KEY, state_id, {
                "cursor": cursor,
                "status": state["status"],
                "migrated": state.get("migrated", 0),
                "skipped": state.get("skipped", 0),
                "failed": state.get("failed", 0),
                "interrupted": False,
                "completed_at": state.get("completed_at"),
                "updated_at": _now(),
            })
            state["id"] = state_id
            return self._migration_result(state, resumed=resumed)

    def resume_payload_migration(self, **kwargs: Any) -> dict[str, Any]:
        """续做最近一个未完成的迁移批次。"""
        kwargs.pop("resume", None)
        return self.migrate_legacy_payloads(resume=True, **kwargs)

    def _migration_snapshot(self) -> list[dict[str, Any]]:
        rows = json_store.get_all(self.MODULE)
        unconfirmed = [
            row for row in rows
            if not self._is_confirmed(row) and isinstance(row.get("原始消息体"), str)
        ]
        return sorted(unconfirmed, key=lambda row: int(row.get("id", 0)))

    @staticmethod
    def _is_confirmed(entry: dict[str, Any]) -> bool:
        if entry.get("迁移状态") == "已确认":
            return is_versioned_payload(entry.get("死信载荷"))
        if entry.get("迁移状态") == "原生版本化":
            return is_versioned_payload(entry.get("死信载荷"))
        return False

    def _find_migration(self, batch_id: str) -> dict[str, Any] | None:
        for row in json_store.get_all(MIGRATION_KEY):
            if row.get("batch_id") == batch_id:
                return dict(row)
        return None

    def _latest_migration(self, statuses: tuple[str, ...] | None = None) -> dict[str, Any] | None:
        rows = json_store.get_all(MIGRATION_KEY)
        if statuses is not None:
            rows = [row for row in rows if row.get("status") in statuses]
        if not rows:
            return None
        return dict(max(rows, key=lambda row: int(row.get("id", 0))))

    @staticmethod
    def _migration_result(state: dict[str, Any], *, resumed: bool) -> dict[str, Any]:
        cursor = int(state.get("cursor", 0))
        total = int(state.get("total", 0))
        return {
            "ok": True,
            "batch_id": state.get("batch_id"),
            "status": state.get("status"),
            "total": total,
            "cursor": cursor,
            "remaining": max(total - cursor, 0),
            "migrated": int(state.get("migrated", 0)),
            "skipped": int(state.get("skipped", 0)),
            "failed": int(state.get("failed", 0)),
            "interrupted": bool(state.get("interrupted")),
            "resumed": resumed,
            "message": _migration_message(state),
        }

    def migration_status(self, batch_id: str | None = None) -> dict[str, Any]:
        """查询迁移批次状态；不传批次时返回最近一批。"""
        state = self._find_migration(batch_id) if batch_id else self._latest_migration()
        if state is None:
            return {"ok": False, "message": "没有迁移批次", "batch_id": batch_id}
        result = self._migration_result(state, resumed=bool(state.get("resumed", False)))
        result["updated_at"] = state.get("updated_at")
        result["started_at"] = state.get("started_at")
        return result

    def rollback_payload_migration(
        self,
        *,
        batch_id: str | None = None,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any]:
        """回滚迁移：移除迁移写入的版本化副本，保留原文与处置凭证。

        只回滚由迁移写出的副本；升级后原生入队的版本化载荷不受影响。
        回滚后旧版消费者仍可读 ``原始消息体``，新版消费者回退按原文解释。
        """
        with _MIGRATION_LOCK:
            state = self._find_migration(batch_id) if batch_id else self._latest_migration()
            target_batch = state.get("batch_id") if state else batch_id
            if state and state.get("status") == "已回滚":
                return {
                    "ok": True,
                    "batch_id": target_batch,
                    "status": "已回滚",
                    "rolled_back": 0,
                    "message": "该批次已回滚",
                }

            rolled_back = 0
            for entry in json_store.get_all(self.MODULE):
                if entry.get("迁移状态") not in ("已写出", "已确认"):
                    continue
                if target_batch and entry.get("迁移批次") != target_batch:
                    continue
                payload = entry.get("死信载荷")
                if not (isinstance(payload, dict) and payload.get("legacy_origin")):
                    continue
                json_store.update(self.MODULE, int(entry["id"]), {
                    "死信载荷": None,
                    "载荷版本": None,
                    "载荷类型": None,
                    "迁移状态": "已回滚",
                    "回滚批次": target_batch,
                    "回滚时间": _now(),
                })
                rolled_back += 1

            if state:
                json_store.update(MIGRATION_KEY, int(state["id"]), {
                    "status": "已回滚",
                    "rolled_back": rolled_back,
                    "rolled_back_at": _now(),
                    "rollback_deployment_version": deployment_version,
                    "updated_at": _now(),
                })
            return {
                "ok": True,
                "batch_id": target_batch,
                "status": "已回滚",
                "rolled_back": rolled_back,
                "message": f"已回滚 {rolled_back} 条迁移载荷，原文与处置凭证保持可用",
            }

    # ------------------------------------------------------------------
    # 重放：按版本消费、写回复核清单、并发只确认一份实际结果
    # ------------------------------------------------------------------
    def replay_dead_letter(
        self,
        dlq_id: int,
        *,
        batch_id: str | None = None,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any]:
        """重放单条死信；并发调用时只有第一次会产生实际消费。"""
        with _REPLAY_LOCK:
            entry = json_store.get_by_id(self.MODULE, dlq_id)
            if entry is None:
                return {"dlq_id": dlq_id, "ok": False, "reason": "死信记录不存在"}

            receipt = self.get_receipt(dlq_id)
            if receipt and receipt.get("actual_consumption_id"):
                ledger = self._latest_confirmed_review(dlq_id)
                return {
                    "dlq_id": dlq_id,
                    "ok": True,
                    "deduplicated": True,
                    "message": "该死信已确认实际消费结果，未重复消费",
                    "actual_consumption_id": receipt["actual_consumption_id"],
                    "payload_version": receipt.get("actual_consumption_payload_version"),
                    "receipt_id": receipt.get("receipt_id"),
                    "review": ledger,
                }

            if entry.get("status") == "已丢弃":
                return {"dlq_id": dlq_id, "ok": False, "reason": "记录已丢弃，不可重放"}
            if entry.get("status") == "已重放":
                return {"dlq_id": dlq_id, "ok": False, "reason": "记录已重放，且没有可关联的实际消费凭证"}

            payload_view = self.get_payload_for_replay(dlq_id, deployment_version=deployment_version)
            if not payload_view.get("ok"):
                return {"dlq_id": dlq_id, "ok": False, "reason": payload_view.get("message", "载荷不可用")}

            # 存量记录可能还没有凭证：重放前按原死信编号补一份，保证事后可对账。
            # 用入队时间作为确认时间，保留原有处置时间线。
            receipt = self._ensure_receipt(
                entry,
                payload_view["payload"],
                deployment_version=deployment_version,
                confirmed_at=entry.get("入队时间") or entry.get("created_at"),
            )

            # 跨模块：消息队列消费按所用版本取得内容、执行消费并写回复核清单。
            from app.services.mq_consumer import MqConsumerService

            consumer = MqConsumerService()
            consumption = consumer.consume_replayed_dead_letter(
                dlq_id,
                deployment_version=deployment_version,
                batch_id=batch_id or "",
            )
            payload_view = consumption.get("payload_view", payload_view)
            now = _now()
            actual_id = consumption.get("actual_consumption_id")

            review = consumption.get("review")
            if review is None:
                # 消费在入口就被拒绝（版本不可用等），仍记一条失败复核。
                review = self._append_review(
                    entry=entry,
                    payload_view=payload_view,
                    consumption=consumption,
                    batch_id=batch_id or "",
                    deployment_version=deployment_version,
                )

            if consumption.get("ok") and actual_id:
                json_store.update(self.MODULE, dlq_id, {
                    "status": "已重放",
                    "处置状态": "已重放",
                    "pending": False,
                    "abnormal": False,
                    "重放批次": batch_id or "",
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "replayed_at": now,
                    "实际消费编号": actual_id,
                    "重放载荷版本": payload_view.get("payload_version"),
                })
                self._update_receipt(dlq_id, {
                    "actual_consumption_id": actual_id,
                    "actual_consumption_payload_version": payload_view.get("payload_version"),
                    "actual_consumption_at": now,
                    "replay_deployment_version": deployment_version,
                    "receipt_status": "已重放",
                    "review_id": review.get("review_id") if review else None,
                })
                return {
                    "dlq_id": dlq_id,
                    "ok": True,
                    "deduplicated": False,
                    "message": "重放成功，已写回死信复核清单",
                    "actual_consumption_id": actual_id,
                    "payload_version": payload_view.get("payload_version"),
                    "payload_type": payload_view.get("payload_type"),
                    "received_payload": consumption.get("received_payload"),
                    "receipt_id": receipt.get("receipt_id") if receipt else None,
                    "review": review,
                }

            json_store.update(self.MODULE, dlq_id, {
                "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                "status": "待处置",
                "失败原因": str(entry.get("失败原因", "")) + " [重放重试]",
                "updated_at": now,
            })
            self._update_receipt(dlq_id, {
                "last_attempt_at": now,
                "last_attempt_deployment_version": deployment_version,
                "last_attempt_result": "失败",
            })
            return {
                "dlq_id": dlq_id,
                "ok": False,
                "deduplicated": False,
                "reason": consumption.get("message", "重放执行失败"),
                "payload_version": payload_view.get("payload_version"),
                "review": review,
            }

    def record_review_entry(
        self,
        dlq_id: int,
        *,
        payload_view: dict[str, Any],
        consumption: dict[str, Any],
        batch_id: str = "",
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any] | None:
        """跨模块入口：消息队列消费把结果写回死信复核清单。"""
        entry = json_store.get_by_id(self.MODULE, dlq_id)
        if entry is None:
            return None
        return self._append_review(
            entry=entry,
            payload_view=payload_view,
            consumption=consumption,
            batch_id=batch_id,
            deployment_version=deployment_version,
        )

    def _append_review(
        self,
        *,
        entry: dict[str, Any],
        payload_view: dict[str, Any],
        consumption: dict[str, Any],
        batch_id: str,
        deployment_version: str,
    ) -> dict[str, Any] | None:
        if not payload_view or payload_view.get("payload_version") is None:
            # 载荷无法解析时不写复核清单，失败原因留在重放结果里。
            return None
        review_id = (
            f"RVW-{entry.get('记录编号', entry.get('id'))}-"
            f"{uuid.uuid4().hex[:8].upper()}"
        )
        success = bool(consumption.get("ok"))
        row = {
            "review_id": review_id,
            "复核编号": review_id,
            "dlq_id": int(entry.get("id", 0)),
            "记录编号": entry.get("记录编号", ""),
            "原始队列": entry.get("原始队列", ""),
            "batch_id": batch_id,
            "payload_version": payload_view.get("payload_version"),
            "载荷版本": payload_view.get("payload_version"),
            "payload_type": payload_view.get("payload_type"),
            "payload_source": payload_view.get("source"),
            "payload_digest": payload_view.get("digest"),
            "received_payload": consumption.get("received_payload"),
            "received_text": payload_view.get("raw_text"),
            "consumer_version": consumption.get("consumer_version", deployment_version),
            "deployment_version": deployment_version,
            "result_status": "已确认" if success else "失败",
            "消费结果": "已确认" if success else "失败",
            "message": consumption.get("message", ""),
            "actual_consumption_id": consumption.get("actual_consumption_id"),
            "实际消费编号": consumption.get("actual_consumption_id"),
            "receipt_id": (self.get_receipt(int(entry.get("id", 0))) or {}).get("receipt_id"),
            "created_at": _now(),
        }
        json_store.insert(REVIEW_KEY, row)
        return row

    def list_review_ledger(self, dlq_id: int | None = None) -> list[dict[str, Any]]:
        rows = json_store.get_all(REVIEW_KEY)
        if dlq_id is not None:
            rows = [row for row in rows if int(row.get("dlq_id", 0)) == int(dlq_id)]
        return sorted(rows, key=lambda row: int(row.get("id", 0)))

    def _latest_confirmed_review(self, dlq_id: int) -> dict[str, Any] | None:
        rows = [
            row for row in self.list_review_ledger(dlq_id)
            if row.get("result_status") == "已确认"
        ]
        return rows[-1] if rows else None

    def batch_replay(
        self,
        dlq_ids: list[int],
        *,
        deployment_version: str = DEPLOYMENT_VERSION,
    ) -> dict[str, Any]:
        """批量重放死信队列中的消息。

        每条记录都经由消息队列消费服务按载荷声明的版本消费，结果写入
        死信复核清单；同一编号并发重放时只有一份实际结果被确认。
        """
        if not dlq_ids:
            return {"ok": False, "message": "未指定待重放的死信记录", "replayed": 0, "failed": 0}

        batch_id = f"REPLAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:4]}"
        results = []
        replayed = 0
        replay_failed = 0

        for dlq_id in dlq_ids:
            outcome = self.replay_dead_letter(
                int(dlq_id),
                batch_id=batch_id,
                deployment_version=deployment_version,
            )
            if outcome.get("ok"):
                replayed += 1
                item = {
                    "dlq_id": dlq_id,
                    "ok": True,
                    "message": outcome.get("message", "重放成功"),
                    "actual_consumption_id": outcome.get("actual_consumption_id"),
                    "payload_version": outcome.get("payload_version"),
                    "deduplicated": bool(outcome.get("deduplicated")),
                }
            else:
                replay_failed += 1
                item = {
                    "dlq_id": dlq_id,
                    "ok": False,
                    "reason": outcome.get("reason", "重放执行失败"),
                }
            results.append(item)

        self._replay_history.append({
            "batch_id": batch_id,
            "total": len(dlq_ids),
            "replayed": replayed,
            "failed": replay_failed,
            "deployment_version": deployment_version,
            "timestamp": _now(),
        })

        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            tracer._record_trace(
                endpoint_id=0,
                action="死信批量重放",
                latency_ms=200,
                status="正常" if replay_failed == 0 else "异常",
                record_count=replayed,
            )
        except Exception:
            pass

        return {
            "ok": True,
            "message": f"批量重放完成: 成功 {replayed}/{len(dlq_ids)}, 失败 {replay_failed}",
            "batch_id": batch_id,
            "replayed": replayed,
            "failed": replay_failed,
            "total": len(dlq_ids),
            "review_ledger": f"{REVIEW_KEY}",
            "results": results,
        }

    # ------------------------------------------------------------------
    def analyze_failure_reasons(self) -> dict[str, Any]:
        """分析所有死信记录的失败原因分布，提供分类统计和主要问题识别。"""
        rows = json_store.get_all(self.MODULE)
        if not rows:
            return {"total": 0, "categories": {}, "top_reasons": []}

        reason_counter = Counter()
        source_counter = Counter()

        for row in rows:
            reason = str(row.get("失败原因", "未知"))
            source = str(row.get("原始队列", "未知"))
            reason_counter[reason] += 1
            source_counter[source] += 1

        categories = {
            "超时类": sum(1 for r, c in reason_counter.items() if "超时" in r or "timeout" in r),
            "格式类": sum(1 for r, c in reason_counter.items() if "格式" in r or "format" in r or "序列化" in r),
            "数据类": sum(1 for r, c in reason_counter.items() if "数据" in r or "data" in r or "空" in r),
            "连接类": sum(1 for r, c in reason_counter.items() if "连接" in r or "网络" in r or "connect" in r),
            "认证类": sum(1 for r, c in reason_counter.items() if "认证" in r or "auth" in r or "权限" in r),
            "其他": sum(1 for r, c in reason_counter.items() if not any(kw in r for kw in ["超时", "timeout", "格式", "format", "序列化", "数据", "data", "空", "连接", "网络", "connect", "认证", "auth", "权限"])),
        }

        return {
            "total": len(rows),
            "categories": {k: v for k, v in categories.items() if v > 0},
            "top_reasons": reason_counter.most_common(5),
            "top_sources": source_counter.most_common(5),
            "pending_count": sum(1 for r in rows if r.get("status") in ("草稿", "待处置")),
        }

    def get_dlq_stats(self) -> dict[str, Any]:
        """获取死信队列的全局统计信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {}
        by_queue = {}
        by_payload_version: dict[str, int] = {}
        for row in rows:
            s = row.get("status", "未知")
            q = row.get("原始队列", "未知")
            pv = row.get("载荷版本")
            by_status[s] = by_status.get(s, 0) + 1
            by_queue[q] = by_queue.get(q, 0) + 1
            key = str(pv) if pv is not None else "未版本化"
            by_payload_version[key] = by_payload_version.get(key, 0) + 1

        return {
            "total_dlq": len(rows),
            "by_status": by_status,
            "by_queue": dict(sorted(by_queue.items(), key=lambda x: x[1], reverse=True)[:10]),
            "by_payload_version": by_payload_version,
            "pending_dispatch": by_status.get("草稿", 0) + by_status.get("待处置", 0),
            "replayed_count": by_status.get("已重放", 0),
            "discarded_count": by_status.get("已丢弃", 0),
            "review_entries": len(json_store.get_all(REVIEW_KEY)),
            "receipts": len(json_store.get_all(RECEIPT_KEY)),
            "analysis": self.analyze_failure_reasons(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出死信队列管理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        analysis = self.analyze_failure_reasons()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信队列记录数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_replays": len(self._replay_history),
            "failure_categories": analysis.get("categories", {}),
            "versioned_payload_count": sum(1 for r in rows if is_versioned_payload(r.get("死信载荷"))),
            "migration_batches": len(json_store.get_all(MIGRATION_KEY)),
            "review_entries": len(json_store.get_all(REVIEW_KEY)),
            "operation": self.OPERATION_TERM,
        }
