"""版本化死信载荷迁移与重放的验收测试。

覆盖需求中的关键场景：

* 存量字符串载荷迁移为版本化载荷，普通文本逐字保留；
* 形似 JSON 的旧文本仍按原文重放，真正的结构化消息按声明版本解释；
* 迁移中断后可续做，消息身份不变；
* 副本已写出但迁移未确认时，从原死信编号发起重放，核对消费入口载荷；
* 迁移完成或回滚后，不同部署版本仍能关联到同一次实际消费；
* 同一死信编号并发重放只确认一份实际结果；
* 新旧数据同时存在时重放仍然正确。
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
from app.persistence import JsonStore
import app.services.call_trace_log as call_trace_mod
import app.services.dlq_handler as dlq_mod
import app.services.mq_consumer as mq_mod
import app.store as seed_store_mod
from app.services.dlq_handler import DlqHandlerService
from app.services.dlq_payload import (
    PAYLOAD_SCHEMA_VERSION,
    PayloadVersionError,
    interpret_for_consumption,
)
from app.services.mq_consumer import MqConsumerService

# 这些模块在导入时绑定了 json_store 全局单例；测试里要换成临时存储。
_PATCHED_MODULES = (dlq_mod, mq_mod, call_trace_mod)


class DlqPayloadTestCase(unittest.TestCase):
    """每个用例使用独立的临时存储，避免污染仓库内数据。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._store = JsonStore(base_dir=self._tmp.name)
        self._original_store = app.persistence.json_store
        self._original_refs = [module.json_store for module in _PATCHED_MODULES]
        app.persistence.json_store = self._store
        for module in _PATCHED_MODULES:
            module.json_store = self._store
        # 清空示例仓库里的死信样例，避免和临时存储的同号记录混淆。
        self._seed_rows_backup = list(seed_store_mod.store.rows("dlq_handler"))
        seed_store_mod.store.rows("dlq_handler").clear()
        self.dlq = DlqHandlerService()
        self.mq = MqConsumerService()

    def tearDown(self):
        seed_store_mod.store.rows("dlq_handler").extend(self._seed_rows_backup)
        app.persistence.json_store = self._original_store
        for module, original in zip(_PATCHED_MODULES, self._original_refs):
            module.json_store = original
        self._tmp.cleanup()

    def _seed_legacy(self, texts: list[str], queue: str = "order_queue") -> list[int]:
        """直接写入旧版字符串载荷记录，模拟升级前已存在的死信数据。"""
        ids = []
        for index, text in enumerate(texts):
            row = self._store.insert("dlq_handler", {
                "记录编号": f"DLQ-LEGACY-{index:05d}",
                "原始队列": queue,
                "失败原因": "历史失败",
                "原始消息体": text,
                "重试次数": "0",
                "入队时间": "2026-01-01T00:00:00+00:00",
                "处置状态": "待处置",
                "重放批次": "",
                "status": "待处置",
                "pending": True,
                "abnormal": True,
            })
            ids.append(int(row["id"]))
        return ids

    def _entry(self, dlq_id: int) -> dict:
        return self._store.get_by_id("dlq_handler", dlq_id)

    def _open_record(self, dlq_id: int) -> dict:
        """模拟“重新打开处置记录”：带上凭证与复核清单。"""
        return self.dlq.get_entry(dlq_id) or {}


class TestVersionedEnqueue(DlqPayloadTestCase):
    def test_new_message_is_versioned_object(self):
        result = self.dlq.enqueue_dead_letter(
            "order_queue", '{"id": 123}', "格式异常", 1,
        )
        self.assertTrue(result["ok"])
        entry = self._entry(result["dlq_id"])
        payload = entry["死信载荷"]
        self.assertIsInstance(payload, dict)
        self.assertEqual(payload["payload_version"], PAYLOAD_SCHEMA_VERSION)
        self.assertEqual(payload["payload_type"], "text")
        # 普通文本逐字保留
        self.assertEqual(payload["body"], '{"id": 123}')
        self.assertEqual(payload["raw_text"], '{"id": 123}')
        self.assertEqual(entry["原始消息体"], '{"id": 123}')
        self.assertEqual(entry["迁移状态"], "原生版本化")

    def test_structured_message_uses_declared_version(self):
        result = self.dlq.enqueue_dead_letter(
            "order_queue",
            original_payload={"type": "data_event", "data": {"id": 7}},
            payload_type="structured",
            raw_text='{"type":"data_event","data":{"id":7}}',
            failure_reason="处理超时",
        )
        entry = self._entry(result["dlq_id"])
        self.assertEqual(entry["死信载荷"]["payload_type"], "structured")
        replay = self.dlq.replay_dead_letter(result["dlq_id"])
        self.assertTrue(replay["ok"])
        # v2 结构化消息以 data 字段承载业务数据
        self.assertEqual(replay["received_payload"], {"id": 7})
        self.assertEqual(replay["payload_version"], PAYLOAD_SCHEMA_VERSION)

    def test_unsupported_version_rejected(self):
        with self.assertRaises(PayloadVersionError):
            self.dlq.enqueue_dead_letter(
                "q", "hello", "失败", payload_version=99,
            )


class TestMigration(DlqPayloadTestCase):
    def test_migration_preserves_text_identity(self):
        text = "普通文本，不是 JSON"
        [dlq_id] = self._seed_legacy([text])
        before = self._entry(dlq_id)

        result = self.dlq.migrate_legacy_payloads()
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "已完成")
        self.assertEqual(result["migrated"], 1)

        after = self._entry(dlq_id)
        # 消息身份不变
        for field in ("id", "记录编号", "原始队列", "入队时间"):
            self.assertEqual(before[field], after[field])
        self.assertEqual(after["迁移状态"], "已确认")
        payload = after["死信载荷"]
        self.assertEqual(payload["payload_version"], PAYLOAD_SCHEMA_VERSION)
        self.assertEqual(payload["body"], text)
        self.assertEqual(payload["raw_text"], text)
        self.assertTrue(payload["legacy_origin"])
        # 原文字段仍在，旧版消费者可继续定位
        self.assertEqual(after["原始消息体"], text)

    def test_interrupted_migration_resumes(self):
        texts = [f'{{"id": {i}}}' for i in range(5)]
        ids = self._seed_legacy(texts)

        first = self.dlq.migrate_legacy_payloads(interrupt_after=2)
        self.assertEqual(first["status"], "已中断")
        self.assertTrue(first["interrupted"])
        batch_id = first["batch_id"]
        self.assertEqual(first["cursor"], 1)  # 第二条已写出、未确认

        # 已写出的副本先在磁盘上，尚未确认
        written = self._entry(ids[1])
        self.assertEqual(written["迁移状态"], "已写出")
        self.assertTrue(written["死信载荷"])

        resumed = self.dlq.resume_payload_migration()
        self.assertTrue(resumed["ok"])
        self.assertEqual(resumed["batch_id"], batch_id)
        self.assertEqual(resumed["status"], "已完成")
        self.assertTrue(resumed["resumed"])
        for dlq_id in ids:
            self.assertEqual(self._entry(dlq_id)["迁移状态"], "已确认")

    def test_resume_by_batch_id(self):
        ids = self._seed_legacy(["a", "b", "c"])
        first = self.dlq.migrate_legacy_payloads(interrupt_after=1)
        second = self.dlq.migrate_legacy_payloads(batch_id=first["batch_id"])
        self.assertEqual(second["status"], "已完成")
        self.assertEqual(second["migrated"] + second["skipped"], len(ids))

    def test_migration_is_idempotent(self):
        self._seed_legacy(["x", "y"])
        first = self.dlq.migrate_legacy_payloads()
        self.assertEqual(first["migrated"], 2)
        # 再发起一次，已确认的会跳过
        second = self.dlq.migrate_legacy_payloads()
        self.assertEqual(second["migrated"], 0)
        self.assertEqual(second["status"], "已完成")

    def test_rollback_keeps_receipts_and_original_text(self):
        text = '{"legacy": true}'
        [dlq_id] = self._seed_legacy([text])
        self.dlq.enqueue_dead_letter("order_queue", "another", "失败")
        # 存量记录补处置凭证（模拟升级时已生成）
        from app.services.dlq_payload import text_payload
        receipt = self.dlq._create_receipt(
            self._entry(dlq_id),
            text_payload(text),
            deployment_version="dlq-v1",
        )
        self.assertTrue(receipt["receipt_id"])

        migration = self.dlq.migrate_legacy_payloads()
        rollback = self.dlq.rollback_payload_migration(batch_id=migration["batch_id"])
        self.assertTrue(rollback["ok"])
        self.assertEqual(rollback["rolled_back"], 1)

        entry = self._entry(dlq_id)
        self.assertEqual(entry["迁移状态"], "已回滚")
        self.assertIsNone(entry["死信载荷"])
        self.assertEqual(entry["原始消息体"], text)
        # 凭证仍可按原死信编号找到
        receipt_after = self.dlq.get_receipt(dlq_id, deployment_version="dlq-v2")
        self.assertIsNotNone(receipt_after)
        self.assertEqual(receipt_after["记录编号"], entry["记录编号"])
        self.assertEqual(receipt_after["dlq_id"], dlq_id)

        # 回滚后仍可按原文重放
        replay = self.dlq.replay_dead_letter(dlq_id, deployment_version="dlq-v2")
        self.assertTrue(replay["ok"])
        self.assertEqual(replay["received_payload"], text)
        self.assertEqual(replay["payload_type"], "text")


class TestReplayWithBothCopies(DlqPayloadTestCase):
    def test_replay_while_copy_written_but_unconfirmed(self):
        text = '{"seemingly": "json", "n": 1}'
        [dlq_id] = self._seed_legacy([text])
        migration = self.dlq.migrate_legacy_payloads(interrupt_after=1)
        self.assertEqual(migration["status"], "已中断")

        entry = self._entry(dlq_id)
        self.assertEqual(entry["迁移状态"], "已写出")
        self.assertTrue(entry["死信载荷"])
        self.assertEqual(entry["原始消息体"], text)

        # 从原死信编号发起重放，核对消费入口实际得到的载荷
        payload_view = self.mq.consume_replayed_dead_letter(dlq_id)
        self.assertTrue(payload_view["ok"])
        self.assertEqual(payload_view["payload_version"], PAYLOAD_SCHEMA_VERSION)
        self.assertEqual(payload_view["payload_type"], "text")
        self.assertEqual(payload_view["received_payload"], text)
        self.assertIn("versioned", payload_view["payload_view"]["source"])

        replay = self.dlq.replay_dead_letter(dlq_id)
        self.assertTrue(replay["ok"])
        self.assertEqual(replay["received_payload"], text)

    def test_resolve_falls_back_when_copy_conflicts(self):
        text = "original"
        [dlq_id] = self._seed_legacy([text])
        self.dlq.migrate_legacy_payloads()
        # 人为制造不一致的副本
        self._store.update("dlq_handler", dlq_id, {
            "死信载荷": {"payload_version": 2, "payload_type": "text",
                        "body": "tampered", "raw_text": "tampered"},
        })
        view = self.dlq.resolve_payload(dlq_id)
        self.assertEqual(view["source"], "legacy")
        self.assertEqual(view["raw_text"], text)
        self.assertIn("不一致", view["warning"])

    def test_unknown_version_is_not_guessed(self):
        [dlq_id] = self._seed_legacy(["text"])
        self.dlq.migrate_legacy_payloads()
        self._store.update("dlq_handler", dlq_id, {
            "死信载荷": {"payload_version": 99, "payload_type": "text",
                        "body": "text", "raw_text": "text"},
        })
        view = self.dlq.resolve_payload(dlq_id)
        self.assertEqual(view["source"], "legacy")
        replay = self.dlq.replay_dead_letter(dlq_id)
        self.assertTrue(replay["ok"])
        self.assertEqual(replay["received_payload"], "text")

    def test_native_unsupported_version_rejected(self):
        """原生版本化消息声明了不支持的版本：拒绝重放，不按原文猜测。"""
        row = self._store.insert("dlq_handler", {
            "记录编号": "DLQ-NATIVE-00001",
            "原始队列": "order_queue",
            "失败原因": "历史失败",
            "原始消息体": "body",
            "死信载荷": {"payload_version": 99, "payload_type": "text",
                        "body": "body", "raw_text": "body"},
            "迁移状态": "原生版本化",
            "status": "待处置",
            "pending": True,
            "abnormal": True,
        })
        view = self.dlq.resolve_payload(int(row["id"]))
        self.assertEqual(view["source"], "unresolved")
        replay = self.dlq.replay_dead_letter(int(row["id"]))
        self.assertFalse(replay["ok"])
        self.assertIn("不支持", replay["reason"])
        self.assertEqual(self.dlq.list_review_ledger(int(row["id"])), [])

    def test_json_like_legacy_text_replays_as_text(self):
        text = '{"order_id": "A-1", "amount": 10}'
        [dlq_id] = self._seed_legacy([text])
        replay = self.dlq.replay_dead_letter(dlq_id)
        self.assertTrue(replay["ok"])
        self.assertEqual(replay["payload_type"], "text")
        self.assertEqual(replay["received_payload"], text)
        self.assertIsInstance(replay["received_payload"], str)

    def test_structured_v2_interpreted_as_object(self):
        view = interpret_for_consumption({
            "payload_version": 2,
            "payload_type": "structured",
            "body": {"type": "event", "data": {"id": 1}},
        })
        self.assertEqual(view["payload_type"], "structured")
        self.assertEqual(view["received_payload"]["data"], {"id": 1})


class TestReceiptAndLedger(DlqPayloadTestCase):
    def test_receipt_survives_new_deployment_version(self):
        result = self.dlq.enqueue_dead_letter("order_queue", "hello", "失败")
        dlq_id = result["dlq_id"]
        receipt = self.dlq.get_receipt(dlq_id, deployment_version="dlq-v3")
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt["记录编号"], result["record_no"])
        self.assertEqual(receipt["opened_by_deployment_version"], "dlq-v3")
        # 新部署版本按记录编号同样能定位
        by_no = self.dlq.find_receipt(result["record_no"])
        self.assertEqual(by_no["dlq_id"], dlq_id)

    def test_replay_writes_review_ledger(self):
        result = self.dlq.enqueue_dead_letter("order_queue", "hello", "失败")
        replay = self.dlq.replay_dead_letter(result["dlq_id"], deployment_version="dlq-v2")
        self.assertTrue(replay["ok"])
        ledger = self.dlq.list_review_ledger(result["dlq_id"])
        self.assertEqual(len(ledger), 1)
        row = ledger[0]
        self.assertEqual(row["payload_version"], PAYLOAD_SCHEMA_VERSION)
        self.assertEqual(row["result_status"], "已确认")
        self.assertEqual(row["actual_consumption_id"], replay["actual_consumption_id"])
        self.assertEqual(row["deployment_version"], "dlq-v2")

        receipt = self.dlq.get_receipt(result["dlq_id"])
        self.assertEqual(receipt["actual_consumption_id"], replay["actual_consumption_id"])
        self.assertEqual(receipt["replay_deployment_version"], "dlq-v2")

    def test_concurrent_replay_confirms_single_result(self):
        result = self.dlq.enqueue_dead_letter("order_queue", "hello", "失败")
        dlq_id = result["dlq_id"]
        outcomes: list[dict] = []
        barrier = threading.Barrier(6)

        def worker():
            barrier.wait()
            outcomes.append(self.dlq.replay_dead_letter(dlq_id))

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        confirmed = [o for o in outcomes if o.get("ok") and o.get("actual_consumption_id")]
        actual_ids = {o["actual_consumption_id"] for o in confirmed}
        self.assertEqual(len(actual_ids), 1, outcomes)
        # 只有一份被确认的实际消费记录
        confirmed_rows = [
            row for row in self.dlq.list_review_ledger(dlq_id)
            if row["result_status"] == "已确认"
        ]
        self.assertEqual(len(confirmed_rows), 1)
        receipt = self.dlq.get_receipt(dlq_id)
        self.assertEqual(receipt["actual_consumption_id"], actual_ids.pop())

    def test_resume_by_batch_id(self):
        ids = self._seed_legacy(["a", "b", "c"])
        first = self.dlq.migrate_legacy_payloads(interrupt_after=1)
        second = self.dlq.migrate_legacy_payloads(batch_id=first["batch_id"])
        self.assertEqual(second["status"], "已完成")
        self.assertEqual(second["migrated"] + second["skipped"], len(ids))


class TestRollbackAndReceipt(DlqPayloadTestCase):
    def test_replay_after_migration_and_new_deployment(self):
        [old_id] = self._seed_legacy(['{"id": 42}'])
        self.dlq.migrate_legacy_payloads()
        new = self.dlq.enqueue_dead_letter("order_queue", '{"id": 43}', "失败")

        first = self.dlq.replay_dead_letter(old_id, deployment_version="dlq-v2")
        self.assertTrue(first["ok"])

        # 换一个部署版本重新打开处置记录，仍关联到同一次实际消费
        reopened = self._open_record(old_id)
        self.assertEqual(reopened["处置凭证"]["actual_consumption_id"], first["actual_consumption_id"])
        self.assertEqual(len(reopened["复核清单"]), 1)
        self.assertEqual(reopened["复核清单"][0]["payload_version"], PAYLOAD_SCHEMA_VERSION)

        again = self.dlq.replay_dead_letter(old_id, deployment_version="dlq-v4")
        self.assertTrue(again["deduplicated"])
        self.assertEqual(again["actual_consumption_id"], first["actual_consumption_id"])

        # 新消息（原生版本化）与迁移数据并存
        second = self.dlq.replay_dead_letter(new["dlq_id"], deployment_version="dlq-v4")
        self.assertTrue(second["ok"])
        self.assertEqual(second["received_payload"], '{"id": 43}')
        self.assertNotEqual(second["actual_consumption_id"], first["actual_consumption_id"])

    def test_reopen_after_rollback_keeps_receipt(self):
        text = '{"id": 99}'
        [dlq_id] = self._seed_legacy([text])
        batch = self.dlq.migrate_legacy_payloads()
        reopened = self._open_record(dlq_id)
        # 迁移完成：原文、凭证、版本化副本同时可见
        self.assertEqual(reopened["原始消息体"], text)
        self.assertEqual(reopened["死信载荷"]["payload_version"], PAYLOAD_SCHEMA_VERSION)
        self.assertIn("处置凭证", reopened)

        self.dlq.rollback_payload_migration(batch_id=batch["batch_id"])
        rolled_back = self._open_record(dlq_id)
        self.assertIsNone(rolled_back["死信载荷"])
        self.assertEqual(rolled_back["原始消息体"], text)
        self.assertEqual(
            rolled_back["处置凭证"]["receipt_id"],
            reopened["处置凭证"]["receipt_id"],
        )
        # 回滚后重放仍产生可关联的实际消费
        replay = self.dlq.replay_dead_letter(dlq_id, deployment_version="dlq-v5")
        self.assertTrue(replay["ok"])
        self.assertEqual(replay["received_payload"], text)
        final = self._open_record(dlq_id)
        self.assertEqual(
            final["处置凭证"]["actual_consumption_id"],
            replay["actual_consumption_id"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
