with open("app/api/prompts.py", "r") as f:
    content = f.read()
old = "    clean_url = settings.redis_url.split(\"?\")[0]\n    return aioredis.from_url(clean_url, decode_responses=True, ssl=True, ssl_cert_reqs=None)"
new = "    return aioredis.from_url(settings.redis_url.split(\"?\")[0], decode_responses=True)"
content = content.replace(old, new)

# Wrap all redis calls in promote_version in try/except
old2 = """    redis = get_redis()
    await redis.set(
        _redis_key(app_id, name, \"active\"),
        json.dumps({\"version\": version, \"template\": pv.template}),
    )
    await redis.delete(_redis_key(app_id, name, \"canary\"))
    await redis.aclose()

    logger.info(\"prompt_version_promoted\""""
new2 = """    try:
        redis = get_redis()
        await redis.set(
            _redis_key(app_id, name, \"active\"),
            json.dumps({\"version\": version, \"template\": pv.template}),
        )
        await redis.delete(_redis_key(app_id, name, \"canary\"))
        await redis.aclose()
    except Exception as e:
        logger.warning(\"redis_promote_failed\", error=str(e))

    logger.info(\"prompt_version_promoted\""""
content = content.replace(old2, new2)

with open("app/api/prompts.py", "w") as f:
    f.write(content)
print("Done!")
