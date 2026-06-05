"""add object range columns

Revision ID: 5ce3f7f7ff52
Revises: 26a9efc758a0
Create Date: 2026-06-04 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "5ce3f7f7ff52"
down_revision: Union[str, None] = "26a9efc758a0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        ALTER TABLE runs
        ADD COLUMN object_key TEXT,
        ADD COLUMN object_start BIGINT,
        ADD COLUMN object_end BIGINT;
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        """
        ALTER TABLE runs
        DROP COLUMN object_end,
        DROP COLUMN object_start,
        DROP COLUMN object_key;
        """
    )
