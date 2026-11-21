"""版本化死信载荷：用带显式版本的对象保存消息体，并按声明版本解释。

死信队列最初把消费失败的消息体直接按字符串写入 ``原始消息体``。升级后，
新入队的消息统一保存为 ``死信载荷`` 对象::

    {
        "payload_version": 2,          # 显式版本，决定消费时如何解释
        "payload_type": "text",        # text / structured
        "body": "原始文本或结构化对象",
        "raw_text": "入队时的原始文本",   # 始终保留，用于回放/审计
        "digest": "sha256:...",
        "source": "enqueue" | "migration" | "legacy"
    }

关键约束：

* 文本载荷的 ``body``/``raw_text`` 与原文逐字一致，不做 JSON 解析、不裁剪；
* 形似 JSON 的旧文本在迁移后仍是 ``text``，按原文本重放；
* 只有带显式版本且声明为 ``structured`` 的载荷才按对象解释；
* 未声明版本或版本不受支持时明确报错，绝不静默猜测。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# 当前写出的载荷版本。迁移把存量字符串载荷表示为同一版本下的 text 载荷。
PAYLOAD_SCHEMA_VERSION = 2
# 消费端能够解释的版本；后续新增版本时在这里扩展解释规则。
SUPPORTED_PAYLOAD_VERSIONS = (1, 2)
PAYLOAD_TYPES = ("text", "structured")
# 当前部署版本标识，写入处置凭证/复核清单，用于跨部署版本对账。
DEPLOYMENT_VERSION = "dlq-v2"


class PayloadVersionError(ValueError):
    """载荷版本缺失或不受支持。"""

    def __init__(self, version: Any, message: str) -> None:
        super().__init__(message)
        self.version = version


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def text_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json(value: Any) -> str:
    """结构化内容的稳定文本表示，用于指纹与 ``raw_text`` 兜底。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def text_payload(
    text: str,
    *,
    version: int = PAYLOAD_SCHEMA_VERSION,
    source: str = "enqueue",
    legacy_origin: bool = False,
    migration_batch: str | None = None,
) -> dict[str, Any]:
    """构造文本载荷，文本逐字保留。"""
    if not isinstance(text, str):
        text = str(text)
    payload: dict[str, Any] = {
        "payload_version": int(version),
        "payload_type": "text",
        "body": text,
        "raw_text": text,
        "digest": text_digest(text),
        "source": source,
        "legacy_origin": bool(legacy_origin),
    }
    if migration_batch:
        payload["migration_batch"] = migration_batch
    return payload


def structured_payload(
    body: Any,
    *,
    version: int = PAYLOAD_SCHEMA_VERSION,
    raw_text: str | None = None,
    source: str = "enqueue",
    legacy_origin: bool = False,
) -> dict[str, Any]:
    """构造结构化载荷；``body`` 按原样保存，只附带版本与原文。"""
    if raw_text is None:
        raw_text = canonical_json(body)
    payload: dict[str, Any] = {
        "payload_version": int(version),
        "payload_type": "structured",
        "body": body,
        "raw_text": raw_text,
        "digest": text_digest(raw_text),
        "source": source,
        "legacy_origin": bool(legacy_origin),
    }
    return payload


def is_versioned_payload(value: Any) -> bool:
    """判断一个值是否是带显式版本的载荷对象。"""
    return isinstance(value, dict) and any(
        key in value for key in ("payload_version", "版本", "payloadVersion")
    )


def declared_version(value: Any) -> Any:
    if not isinstance(value, dict):
        return None
    for key in ("payload_version", "版本", "payloadVersion", "version"):
        if key in value:
            return value[key]
    return None


def normalize_payload(value: Any) -> dict[str, Any]:
    """把载荷对象归一化，并校验版本。

    返回 ``{payload_version, payload_type, body, raw_text, digest, source}``。
    文本载荷的 ``raw_text`` 必须是原文，不做任何改写。
    """
    if not isinstance(value, dict):
        raise PayloadVersionError(None, "死信载荷必须是带版本的对象")

    version = declared_version(value)
    if version is None:
        raise PayloadVersionError(None, "死信载荷缺少显式版本")
    try:
        version_number = int(version)
    except (TypeError, ValueError) as exc:
        raise PayloadVersionError(version, f"死信载荷版本无效：{version!r}") from exc
    if version_number not in SUPPORTED_PAYLOAD_VERSIONS:
        raise PayloadVersionError(
            version_number,
            f"不支持的死信载荷版本：{version_number}（支持 {SUPPORTED_PAYLOAD_VERSIONS}）",
        )

    body = value.get("body")
    if body is None and "内容" in value:
        body = value.get("内容")
    if body is None and "payload" in value:
        body = value.get("payload")
    if body is None and "content" in value:
        body = value.get("content")

    payload_type = value.get("payload_type") or value.get("类型")
    if payload_type not in PAYLOAD_TYPES:
        # 未显式声明类型时，只有对象才视为结构化，字符串永远是文本。
        payload_type = "text" if isinstance(body, str) else "structured"

    if payload_type == "text":
        raw_text = value.get("raw_text")
        if raw_text is None:
            raw_text = value.get("原文")
        if raw_text is None:
            raw_text = body
        if not isinstance(raw_text, str):
            raw_text = str(raw_text)
        return {
            "payload_version": version_number,
            "payload_type": "text",
            "body": raw_text,
            "raw_text": raw_text,
            "digest": text_digest(raw_text),
            "source": str(value.get("source") or value.get("来源") or "unknown"),
            "legacy_origin": bool(value.get("legacy_origin", False)),
        }

    raw_text = value.get("raw_text")
    if raw_text is None:
        raw_text = value.get("原文")
    if not isinstance(raw_text, str):
        raw_text = canonical_json(body)
    return {
        "payload_version": version_number,
        "payload_type": "structured",
        "body": body,
        "raw_text": raw_text,
        "digest": text_digest(raw_text),
        "source": str(value.get("source") or value.get("来源") or "unknown"),
        "legacy_origin": bool(value.get("legacy_origin", False)),
    }


def interpret_for_consumption(payload: dict[str, Any]) -> dict[str, Any]:
    """按载荷声明的版本给出消费入口实际应得到的内容。

    * text：返回逐字原文（即使它形似 JSON）；
    * structured：返回结构化对象，由消费者按版本规则处理。
    """
    normalized = normalize_payload(payload)
    if normalized["payload_type"] == "text":
        received: Any = normalized["raw_text"]
        content_type = "text/plain"
    else:
        received = normalized["body"]
        content_type = "application/json"
    return {
        "payload_version": normalized["payload_version"],
        "payload_type": normalized["payload_type"],
        "content_type": content_type,
        "received_payload": received,
        "raw_text": normalized["raw_text"],
        "digest": normalized["digest"],
        "source": normalized["source"],
        "legacy_origin": normalized["legacy_origin"],
    }


def legacy_text_payload(text: str, *, migration_batch: str | None = None) -> dict[str, Any]:
    """把存量字符串表示迁移成版本化 text 载荷；文本逐字保留。"""
    return text_payload(
        text,
        version=PAYLOAD_SCHEMA_VERSION,
        source="migration",
        legacy_origin=True,
        migration_batch=migration_batch,
    )
