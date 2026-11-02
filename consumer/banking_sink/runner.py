"""Continously land kafka CDC partitions, then synchronously commit offsets"""
from __future__ import annotations

import signal
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaException, TopicPartition
from loguru import logger

from .config import Settings
from .events import CDCEvent, parse_event
from .storage import ParquetS3Writer

class KafkaS3Sink:
    def __init__(self, settings: Settings, writer: ParquetS3Writer):
        self.settings = settings
        self.writer = writer
        self.consumer = Consumer({
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group_id,
            "auto.offset.reset": settings.auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": 900000, # 15 minutes; allow blocking uploads / backfills
            "client.id": "banking-kafka-s3-sink",
        })
        self.buffers: dict[tuple[str, int], list[CDCEvent]] = defaultdict(list)
        self.last_flush = time.monotonic()
        self.stopping = False
        self.healthy = True

    def _stop(self, signum, frame) -> None:
        logger.info(f"Received signal {signum}; flushing remaining CDC events")
        self.stopping = True

    def _flush_partition(self, tp: tuple[str, int]) -> None:
        events = self.buffers.get(tp)
        if not events:
            return
        # Never advance kafka offsets before a successful object-store response.
        self.writer.put_batch(events)
        next_offset = events[-1].kafka_offset + 1
        results = self.consumer.commit(
            offsets=[TopicPartition(tp[0], tp[1], next_offset)],
            asynchronous=False,
        )
        for result in results or []:
            if result.error is not None:
                raise KafkaException(result.error)
        logger.info(f"Committed {tp[0]} [{tp[1]}] next offset {next_offset}")
        del self.buffers[tp]

    def _flush_all(self) -> None:
        for tp in list(self.buffers):
            self._flush_partition(tp)
        self.last_flush = time.monotonic()

    def _on_revoke(self, consumer, partitions) -> None:
        logger.info(f"Rebalancing: revoking {partitions}")
        if self.healthy:
            for item in partitions:
                self._flush_partition((item.topic, item.partition))

    def _on_lost(self, consumer, partitions) -> None:
        # Ownership is gone; never commit offsets for lost partitions.
        for item in partitions:
            tp = (item.topic, item.partition)
            if tp in self.buffers:
                logger.warning(f"Lost {tp[0]} [{tp[1]}]; discarding uncommitted in-memory records")
                del self.buffers[tp]

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)
        self.writer.check_bucket()
        logger.info(f"Subscribing to {self.settings.subscriptions} with consumer group {self.settings.consumer_group_id}")
        self.consumer.subscribe(
            list(self.settings.subscriptions),
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        try:
            while not self.stopping:
                message = self.consumer.poll(timeout=1.0)
                if message is not None:
                    if message.error():
                        raise KafkaException(message.error())
                    timestamp_kind, timestamp_ms = message.timestamp()
                    event = parse_event(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                        kafka_timestamp_ms=timestamp_ms if timestamp_kind > 0 else None,
                        key=message.key(),
                        value=message.value(),
                    )
                    tp = (event.topic, event.kafka_partition)
                    self.buffers[tp].append(event)
                    if len(self.buffers[tp]) >= self.settings.batch_size:
                        self._flush_partition(tp)
                if time.monotonic() - self.last_flush >= self.settings.flush_interval_seconds:
                    self._flush_all()
            self._flush_all()
        except Exception:
            self.healthy = False
            logger.exception("Sink stopped on error; uncommitted offsets will be replayed")
            raise
        finally:
            self.healthy = False
            self.consumer.close()
