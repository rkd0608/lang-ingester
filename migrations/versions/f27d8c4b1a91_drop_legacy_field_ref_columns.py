"""drop legacy field ref columns

Revision ID: f27d8c4b1a91
Revises: 5ce3f7f7ff52
Create Date: 2026-06-04 00:00:01.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "f27d8c4b1a91"
down_revision: Union[str, None] = "5ce3f7f7ff52"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        ALTER TABLE runs
        DROP COLUMN metadata,
        DROP COLUMN outputs,
        DROP COLUMN inputs;
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        """
        ALTER TABLE runs
        ADD COLUMN inputs TEXT,
        ADD COLUMN outputs TEXT,
        ADD COLUMN metadata TEXT;
        """
    )
