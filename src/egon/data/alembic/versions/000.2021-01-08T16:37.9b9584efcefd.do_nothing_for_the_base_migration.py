"""Do nothing for the base migration

This migration intentionally does nothing. Downgrading to this revision
should reset the database to a clean state or at least undo everything
done via Alembic.

Revision ID: 9b9584efcefd
Revises:
Create Date: 2021-01-08 16:37:31.238609+00:00

"""

# Revision identifiers, used by Alembic.
revision = "9b9584efcefd"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
