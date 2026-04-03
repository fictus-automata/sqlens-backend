from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings


connect_args: dict = {}
if settings.database_url.startswith("sqlite"):
    # aiosqlite + async engine: keep defaults.
    connect_args = {}

engine = create_async_engine(settings.database_url, pool_pre_ping=True, connect_args=connect_args)

SessionLocal = async_sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as db:
        yield db


async def init_db() -> None:
    """
    For MVP/dev only: create tables from ORM models.

    Production should use migrations.
    """
    from app.db.models import Base  # local import to avoid cycles

    async with engine.begin() as conn:
        if settings.database_url.startswith("sqlite"):
            # Keep SQLite test runs idempotent even if modules are reloaded.
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

