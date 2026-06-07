with open("app/api/proxy.py", "r") as f:
    content = f.read()
old = "    import openai\n    client = openai.AsyncOpenAI(api_key=settings.openai_api_key)"
new = "    import openai\n    groq_key = getattr(settings, \"groq_api_key\", \"\")\n    if groq_key:\n        client = openai.AsyncOpenAI(api_key=groq_key, base_url=\"https://api.groq.com/openai/v1\")\n    else:\n        client = openai.AsyncOpenAI(api_key=settings.openai_api_key)"
if old in content:
    content = content.replace(old, new)
    with open("app/api/proxy.py", "w") as f:
        f.write(content)
    print("Patched successfully!")
else:
    print("Pattern not found - checking current state:")
    import re
    matches = [l for l in content.split("\n") if "AsyncOpenAI" in l]
    for m in matches:
        print(repr(m))
