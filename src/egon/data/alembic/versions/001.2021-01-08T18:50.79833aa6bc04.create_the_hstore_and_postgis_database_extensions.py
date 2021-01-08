"""Create the hstore and postgis database extensions

Revision ID: 79833aa6bc04
Revises: 9b9584efcefd
Create Date: 2021-01-08 18:50:07.951487+00:00

"""
from alembic import op

# Revision identifiers, used by Alembic.
revision = "79833aa6bc04"
down_revision = "9b9584efcefd"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")


def downgrade():
    op.execute("DROP EXTENSION IF EXISTS postgis;")
    op.execute("DROP EXTENSION IF EXISTS hstore;")
