with open("app/api/prompts.py", "r") as f:
    content = f.read()
old = """    redis = get_redis()
    await redis.delete(_redis_key(app_id, name, \"canary\"))
    await redis.set(
        _redis_key(app_id, name, \"active\"),
        json.dumps({\"version\": version, \"template\": pv.template}),
    )
    await redis.aclose()

    logger.warning(\"prompt_version_rollback\""""
new = """    try:
        redis = get_redis()
        await redis.delete(_redis_key(app_id, name, \"canary\"))
        await redis.set(
            _redis_key(app_id, name, \"active\"),
            json.dumps({\"version\": version, \"template\": pv.template}),
        )
        await redis.aclose()
    except Exception as e:
        logger.warning(\"redis_rollback_failed\", error=str(e))

    logger.warning(\"prompt_version_rollback\""""
content = content.replace(old, new)
with open("app/api/prompts.py", "w") as f:
    f.write(content)
print("Done!")
