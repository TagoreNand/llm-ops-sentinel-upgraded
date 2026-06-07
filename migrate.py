import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import text

with open('.env') as f:
    for line in f:
        if line.startswith('DATABASE_URL'):
            db_url = line.split('=', 1)[1].strip().split('?')[0]
            break

engine = create_async_engine(db_url, poolclass=NullPool, connect_args={"ssl": "require"})

statements = [
    "ALTER TABLE prompt_versions ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE review_queue ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE golden_examples ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE drift_baselines ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS golden_match BOOLEAN",
    "ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS golden_similarity FLOAT",
]

async def migrate():
    async with engine.begin() as conn:
        for stmt in statements:
            await conn.execute(text(stmt))
            print("OK:", stmt[:60])
    print("Migration complete!")

asyncio.run(migrate())
