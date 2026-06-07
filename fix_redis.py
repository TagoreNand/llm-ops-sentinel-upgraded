import re
with open("app/api/prompts.py", "r") as f:
    content = f.read()
old = "def get_redis():\n    return aioredis.from_url(settings.redis_url, decode_responses=True)"
new = "def get_redis():\n    import ssl\n    ctx = ssl.create_default_context()\n    ctx.check_hostname = False\n    ctx.verify_mode = ssl.CERT_NONE\n    clean_url = settings.redis_url.split(\"?\")[0]\n    return aioredis.from_url(clean_url, decode_responses=True, ssl_context=ctx)"
content = content.replace(old, new)
with open("app/api/prompts.py", "w") as f:
    f.write(content)
print("Done!")
