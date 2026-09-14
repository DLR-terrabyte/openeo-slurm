"""add processing parameters to argo jobs

Revision ID: 4b8d3a8c1f2e
Revises: 28fe2ce196c8
"""
from alembic import op
import sqlalchemy as sa

revision = "4b8d3a8c1f2e"
down_revision = "28fe2ce196c8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("jobs", sa.Column("processing_parameters", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("jobs", "processing_parameters")
