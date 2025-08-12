"""create runs table

Revision ID: 26a9efc758a0
Revises: 
Create Date: 2025-03-23 11:50:15.855064

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '26a9efc758a0'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
    CREATE TABLE runs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        trace_id UUID NOT NULL,
        name TEXT NOT NULL,
        inputs TEXT,
        outputs TEXT,
        metadata TEXT
    );
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TABLE runs;")
