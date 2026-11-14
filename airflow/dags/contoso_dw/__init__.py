"""SQL for the Contoso data warehouse demo (``contoso_dw`` on the Postgres server).

The ``contoso_orders`` pipeline is plain SQL run through ``SQLExecuteQueryOperator``,
so OpenLineage reads each task's input and output tables from the statements
themselves. Nothing in this package reports lineage by hand.
"""
