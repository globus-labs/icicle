"""Batch gmeta accumulation and Globus Search ingestion."""

from __future__ import annotations

import json
import traceback
from collections.abc import Iterator
from time import monotonic
from typing import Any

from globus_sdk import ClientCredentialsAuthorizer
from globus_sdk import ConfidentialAppAuthClient
from globus_sdk import SearchClient
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream import ProcessFunction
from search import SEARCH_CLIENT_ID
from search import SEARCH_CLIENT_SECRET


def create_search_client() -> SearchClient:
    """Create an authenticated Globus Search client."""
    cc = ConfidentialAppAuthClient(
        client_id=SEARCH_CLIENT_ID,
        client_secret=SEARCH_CLIENT_SECRET,
    )
    authorizer = ClientCredentialsAuthorizer(
        confidential_client=cc,
        scopes='urn:globus:auth:scope:search.api.globus.org:all',
    )
    return SearchClient(authorizer=authorizer)


def exception_to_json(exception: BaseException) -> str:
    """Serialize an exception to a JSON string."""
    exception_dict = {
        'acknowledged': False,
        'success': False,
        'type': exception.__class__.__name__,
        'message': str(exception),
        'traceback': traceback.format_exc().splitlines(),
    }
    return json.dumps(exception_dict)


class BatchGmeta(ProcessFunction):
    """Accumulate gmeta entries and batch-ingest to Globus Search."""

    def __init__(
        self,
        index_id: str,
        max_bs: float,
        max_wt: float,
        mock_ingest: bool,
    ) -> None:
        self.index_id = index_id
        self.max_batch_size = max_bs
        self.max_wait_time = max_wt
        self.mock_ingest = mock_ingest

        self.ingest_batch: list[dict[str, Any]] = []
        self.ingest_batch_size: int = 0
        self.timer_ts: float | None = None
        self.search_client: SearchClient | None = None

    def open(self, ctx: ProcessFunction.Context) -> None:
        """Initialize the search client."""
        self.search_client = create_search_client()

    def close(self) -> None:
        """Flush remaining batch on shutdown."""
        if self.ingest_batch:
            self.perform_ingest()

    def perform_ingest(self) -> str:
        """Send the current batch to Globus Search."""
        data: dict[str, Any] = {
            'ingest_type': 'GMetaList',
            'ingest_data': {'gmeta': self.ingest_batch},
        }

        batch_len = len(self.ingest_batch)
        batch_sz_bytes = self.ingest_batch_size
        batch_sz_mb = batch_sz_bytes / 1024**2

        result: dict[str, Any] = {
            'mock': self.mock_ingest,
            'msg': (
                f'ingested {batch_len} docs'
                f' ({batch_sz_bytes} bytes,'
                f' {batch_sz_mb:.2f} MB)'
            ),
        }

        if self.mock_ingest:
            result['succeed'] = True
            if batch_len == 1:
                result['data'] = data
        else:
            try:
                if self.search_client is None:
                    raise RuntimeError(
                        'Search client not initialized',
                    )
                resp = self.search_client.ingest(
                    self.index_id,
                    data=data,
                )
                result.update(
                    {'succeed': True, 'resp': resp.data},
                )
            except Exception as e:
                result.update(
                    {'succeed': False, 'resp': exception_to_json(e)},
                )

        self.timer_ts = None
        self.ingest_batch = []
        self.ingest_batch_size = 0
        return json.dumps(result)

    def process_element(
        self,
        value: str,
        ctx: KeyedProcessFunction.Context,
    ) -> Iterator[str]:
        """Add element to batch; flush on size or time trigger."""
        self.ingest_batch_size += len(value)
        self.ingest_batch.append(json.loads(value))

        if self.timer_ts is None:
            self.timer_ts = monotonic()

        if self.ingest_batch_size >= self.max_batch_size or (
            self.timer_ts is not None
            and (monotonic() - self.timer_ts >= self.max_wait_time)
        ):
            yield self.perform_ingest()
