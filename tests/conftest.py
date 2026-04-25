import pytest

from ffpts.db import apply_schema, connect


@pytest.fixture
def db():
    """In-memory DuckDB connection with the full schema applied."""
    con = connect(None)
    apply_schema(con)
    yield con
    con.close()
