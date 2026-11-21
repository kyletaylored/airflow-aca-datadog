"""The operator behind contoso_orders' ingest_orders task in stream mode."""
from __future__ import annotations

import os

try:  # Airflow 3
    from airflow.sdk import BaseHook, BaseOperator
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator

from contoso_dw import landing


class LoadLandedOrdersOperator(BaseOperator):
    """Load new landing blobs into raw.orders, and report that lineage to OpenLineage.

    The OpenLineage provider calls ``get_openlineage_facets_on_complete`` on any
    operator that defines it, so the landing zone (input) and raw.orders (output)
    show up on the task like a SQL task's tables do.
    """

    ui_color = "#e8f4ff"

    def __init__(
        self,
        *,
        conn_id: str,
        landing_account: str,
        landing_container: str = "landing",
        landing_conn_env: str = "CONTOSO_LANDING_CONNECTION_STRING",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.landing_account = landing_account
        self.landing_container = landing_container
        self.landing_conn_env = landing_conn_env
        self._loaded_orders: int | None = None

    def execute(self, context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from azure.storage.blob import BlobServiceClient

        container = BlobServiceClient.from_connection_string(
            os.environ[self.landing_conn_env]
        ).get_container_client(self.landing_container)
        # A raw connection, so no hook reports its own lineage for these statements.
        conn = PostgresHook(postgres_conn_id=self.conn_id).get_conn()
        chaos = landing.Chaos.from_params(context["params"])
        try:
            stats, changes = landing.load_landed_orders(conn, container, chaos)
        except landing.SchemaChangeFailed as failed:
            # Closing without a commit rolls the load back.
            self._log_in_span("error", f"{failed}; nothing was loaded", failed.trace_id, failed.span_id)
            raise
        finally:
            conn.close()
        self.log.info(
            "Loaded %(orders)s orders from %(blobs)s landing blobs; %(waiting)s still waiting",
            stats,
        )
        # The load has committed, and each change was checked against information_schema.
        for change in changes:
            self._log_in_span("info", change.describe(), change.trace_id, change.span_id)
        self._loaded_orders = stats["orders"]
        return stats

    def _log_in_span(self, level: str, message: str, trace_id: str | None, span_id: str | None) -> None:
        """Log a line that Datadog links to the ALTER TABLE's span.

        The span has ended by now, and the line may be exported from another process
        (in Airflow 3, the task's supervisor), so it names the span in fields that
        otel_logging_config exports it with.
        """
        log = getattr(self.log, level)
        if trace_id and span_id:
            try:
                log(message, trace_id=trace_id, span_id=span_id)
                return
            except TypeError:  # Airflow 2's stdlib logger takes no fields
                pass
        log(message)

    def get_openlineage_facets_on_complete(self, task_instance):
        from airflow.providers.openlineage.extractors import OperatorLineage
        from openlineage.client.event_v2 import Dataset, OutputDataset
        from openlineage.client.facet_v2 import output_statistics_output_dataset

        conn = BaseHook.get_connection(self.conn_id)
        in_ns, in_name = landing.landing_dataset(self.landing_account, self.landing_container)
        out_ns, out_name = landing.postgres_dataset(conn.host, conn.port, conn.schema, "raw", "orders")
        output_facets = {}
        if self._loaded_orders is not None:
            output_facets["outputStatistics"] = (
                output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                    rowCount=self._loaded_orders
                )
            )
        return OperatorLineage(
            inputs=[Dataset(namespace=in_ns, name=in_name)],
            outputs=[OutputDataset(namespace=out_ns, name=out_name, outputFacets=output_facets)],
        )
