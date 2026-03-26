"""Unit tests for icicle output handlers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from src.icicle.output import JsonFileOutputHandler
from src.icicle.output import KafkaOutputHandler
from src.icicle.output import StdoutOutputHandler

SAMPLE_PAYLOADS = [
    {'op': 'update', 'path': '/tmp/a.txt', 'stat': {'size': 100}},
    {'op': 'delete', 'path': '/tmp/b.txt'},
]


# ---------------------------------------------------------------------------
# StdoutOutputHandler
# ---------------------------------------------------------------------------


class TestStdoutOutputHandler:
    def test_send_prints_json_lines(self, capsys):
        handler = StdoutOutputHandler()
        handler.send(SAMPLE_PAYLOADS)
        output = capsys.readouterr().out
        lines = output.strip().split('\n')
        assert len(lines) == 2
        assert json.loads(lines[0])['op'] == 'update'
        assert json.loads(lines[1])['op'] == 'delete'

    def test_send_empty_list(self, capsys):
        handler = StdoutOutputHandler()
        handler.send([])
        assert capsys.readouterr().out == ''

    def test_close_is_noop(self):
        handler = StdoutOutputHandler()
        handler.close()  # should not raise


# ---------------------------------------------------------------------------
# JsonFileOutputHandler
# ---------------------------------------------------------------------------


class TestJsonFileOutputHandler:
    def test_write_and_read_back(self, tmp_path):
        path = str(tmp_path / 'out.json')
        handler = JsonFileOutputHandler(path)
        handler.send(SAMPLE_PAYLOADS)
        handler.close()

        data = json.loads(Path(path).read_text())
        assert len(data) == 2
        assert data[0]['op'] == 'update'
        assert data[1]['op'] == 'delete'

    def test_multiple_send_calls(self, tmp_path):
        path = str(tmp_path / 'out.json')
        handler = JsonFileOutputHandler(path)
        handler.send([SAMPLE_PAYLOADS[0]])
        handler.send([SAMPLE_PAYLOADS[1]])
        handler.close()

        data = json.loads(Path(path).read_text())
        assert len(data) == 2

    def test_empty_send(self, tmp_path):
        path = str(tmp_path / 'out.json')
        handler = JsonFileOutputHandler(path)
        handler.send([])
        handler.close()

        data = json.loads(Path(path).read_text())
        assert data == []

    def test_close_idempotent(self, tmp_path):
        path = str(tmp_path / 'out.json')
        handler = JsonFileOutputHandler(path)
        handler.send(SAMPLE_PAYLOADS)
        handler.close()
        handler.close()  # should not raise


# ---------------------------------------------------------------------------
# KafkaOutputHandler (unit tests with mocked producer)
# ---------------------------------------------------------------------------


class TestKafkaOutputHandler:
    @patch('confluent_kafka.Producer')
    def test_send_produces_messages(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        handler = KafkaOutputHandler(
            topic='test-topic',
            bootstrap_servers='localhost:9092',
        )
        handler.send(SAMPLE_PAYLOADS)

        # 1 warmup produce in __init__ + 2 from send
        assert mock_producer.produce.call_count == 3
        # Check first send call (index 1, after warmup)
        first_send = mock_producer.produce.call_args_list[1]
        assert first_send[0][0] == 'test-topic'
        payload = json.loads(first_send[1]['value'].decode())
        assert payload['op'] == 'update'
        # Check poll was called
        mock_producer.poll.assert_called_once_with(0)

    @patch('confluent_kafka.Producer')
    def test_send_empty(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        handler = KafkaOutputHandler(topic='t')
        handler.send([])

        # 1 warmup produce in __init__, none from send([])
        assert mock_producer.produce.call_count == 1
        mock_producer.poll.assert_called_once_with(0)

    @patch('confluent_kafka.Producer')
    def test_close_flushes(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        handler = KafkaOutputHandler(topic='t')
        handler.close()

        # 1 warmup flush in __init__ (timeout=30) + 1 close flush (timeout=10)
        assert mock_producer.flush.call_count == 2
        mock_producer.flush.assert_called_with(timeout=10)

    @patch('confluent_kafka.Producer')
    def test_custom_config(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        KafkaOutputHandler(
            topic='t',
            bootstrap_servers='broker:9093',
            producer_config={'compression.type': 'snappy'},
        )

        conf = mock_producer_cls.call_args[0][0]
        assert conf['bootstrap.servers'] == 'broker:9093'
        assert conf['compression.type'] == 'snappy'

    @patch('confluent_kafka.Producer')
    def test_produce_raises_propagates(self, mock_producer_cls):
        mock_producer = MagicMock()
        # Allow warmup produce, then raise on send
        mock_producer.produce.side_effect = [None, BufferError('queue full')]
        mock_producer_cls.return_value = mock_producer

        handler = KafkaOutputHandler(topic='t')
        with pytest.raises(BufferError, match='queue full'):
            handler.send(SAMPLE_PAYLOADS)

    @patch('confluent_kafka.Producer')
    def test_topic_with_dots_and_hyphens(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        handler = KafkaOutputHandler(topic='my.topic-v2')
        handler.send([SAMPLE_PAYLOADS[0]])

        call = mock_producer.produce.call_args
        assert call[0][0] == 'my.topic-v2'

    @patch('confluent_kafka.Producer')
    def test_producer_config_overrides_bootstrap(self, mock_producer_cls):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        KafkaOutputHandler(
            topic='t',
            bootstrap_servers='a:9092',
            producer_config={'bootstrap.servers': 'b:9092'},
        )

        conf = mock_producer_cls.call_args[0][0]
        assert conf['bootstrap.servers'] == 'b:9092'


# ---------------------------------------------------------------------------
# JsonFileOutputHandler — additional edge cases
# ---------------------------------------------------------------------------


class TestJsonFileOutputHandlerEdgeCases:
    def test_zero_sends_valid_json(self, tmp_path):
        path = str(tmp_path / 'empty.json')
        handler = JsonFileOutputHandler(path)
        handler.close()

        data = json.loads(Path(path).read_text())
        assert data == []

    def test_single_item_no_leading_comma(self, tmp_path):
        path = str(tmp_path / 'single.json')
        handler = JsonFileOutputHandler(path)
        handler.send([SAMPLE_PAYLOADS[0]])
        handler.close()

        data = json.loads(Path(path).read_text())
        assert len(data) == 1
        assert data[0]['op'] == 'update'

    def test_send_after_close_raises(self, tmp_path):
        path = str(tmp_path / 'closed.json')
        handler = JsonFileOutputHandler(path)
        handler.close()

        with pytest.raises(ValueError):
            handler.send([SAMPLE_PAYLOADS[0]])

    def test_100_payloads_valid_json(self, tmp_path):
        """100 payloads — valid JSON array with correct data."""
        path = str(tmp_path / 'volume.json')
        handler = JsonFileOutputHandler(path)
        handler.send(
            [
                {'op': 'update', 'path': f'/f_{i}', 'stat': {'size': i}}
                for i in range(100)
            ],
        )
        handler.close()
        data = json.loads(Path(path).read_text())
        assert len(data) == 100
        assert data[50]['stat']['size'] == 50

    def test_incremental_sends(self, tmp_path):
        """10 separate send() calls — valid JSON."""
        path = str(tmp_path / 'incr.json')
        handler = JsonFileOutputHandler(path)
        for i in range(10):
            handler.send([{'op': 'update', 'path': f'/f_{i}', 'stat': {}}])
        handler.close()
        data = json.loads(Path(path).read_text())
        assert len(data) == 10


# ---------------------------------------------------------------------------
# MSK OAUTHBEARER support
# ---------------------------------------------------------------------------


class TestMskOauthCb:
    @patch('aws_msk_iam_sasl_signer.MSKAuthTokenProvider.generate_auth_token')
    def test_returns_token_and_expiry_seconds(self, mock_gen):
        from src.icicle.output import _msk_oauth_cb

        mock_gen.return_value = ('tok123', 60_000)
        token, expiry = _msk_oauth_cb(None)
        mock_gen.assert_called_once_with('us-east-1')
        assert token == 'tok123'
        assert expiry == 60.0


class TestKafkaOutputHandlerMsk:
    @patch('confluent_kafka.Producer')
    def test_msk_config_passed_to_producer(self, mock_producer_cls):
        from src.icicle.output import _msk_oauth_cb
        from src.icicle.output import _MSK_PRODUCER_DEFAULTS

        mock_producer_cls.return_value = MagicMock()
        producer_config = {**_MSK_PRODUCER_DEFAULTS, 'oauth_cb': _msk_oauth_cb}

        KafkaOutputHandler(
            topic='diaspora-topic',
            bootstrap_servers='b-1:9198,b-2:9198',
            producer_config=producer_config,
        )

        conf = mock_producer_cls.call_args[0][0]
        assert conf['security.protocol'] == 'SASL_SSL'
        assert conf['sasl.mechanisms'] == 'OAUTHBEARER'
        assert conf['compression.type'] == 'snappy'
        assert conf['oauth_cb'] is _msk_oauth_cb
        assert conf['bootstrap.servers'] == 'b-1:9198,b-2:9198'


class TestBuildOutputMsk:
    @patch('confluent_kafka.Producer')
    def test_kafka_msk_uses_diaspora_defaults(self, mock_producer_cls):
        import argparse

        from src.icicle.output import _DIASPORA_BOOTSTRAP_SERVERS

        mock_producer_cls.return_value = MagicMock()
        args = argparse.Namespace(
            kafka='my-topic',
            kafka_bootstrap='localhost:9092',
            kafka_msk=True,
            json=None,
            quiet=False,
        )

        from src.icicle.__main__ import _build_output

        handler = _build_output(args)
        assert isinstance(handler, KafkaOutputHandler)
        conf = mock_producer_cls.call_args[0][0]
        assert conf['bootstrap.servers'] == _DIASPORA_BOOTSTRAP_SERVERS
        assert conf['sasl.mechanisms'] == 'OAUTHBEARER'

    @patch('confluent_kafka.Producer')
    def test_kafka_msk_respects_bootstrap_override(self, mock_producer_cls):
        import argparse

        mock_producer_cls.return_value = MagicMock()
        args = argparse.Namespace(
            kafka='my-topic',
            kafka_bootstrap='custom:9092',
            kafka_msk=True,
            json=None,
            quiet=False,
        )

        from src.icicle.__main__ import _build_output

        handler = _build_output(args)
        assert isinstance(handler, KafkaOutputHandler)
        conf = mock_producer_cls.call_args[0][0]
        assert conf['bootstrap.servers'] == 'custom:9092'

    @patch('confluent_kafka.Producer')
    def test_kafka_without_msk_no_sasl(self, mock_producer_cls):
        import argparse

        mock_producer_cls.return_value = MagicMock()
        args = argparse.Namespace(
            kafka='my-topic',
            kafka_bootstrap='localhost:9092',
            kafka_msk=False,
            json=None,
            quiet=False,
        )

        from src.icicle.__main__ import _build_output

        _build_output(args)
        conf = mock_producer_cls.call_args[0][0]
        assert 'sasl.mechanisms' not in conf
