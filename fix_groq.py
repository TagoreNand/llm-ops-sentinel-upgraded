with open("app/api/proxy.py", "r") as f:
    content = f.read()
old = "async def _call_openai(prompt: str, system: str | None, model: str, max_tokens: int, temperature: float) -> dict:\n    \"\"\"Make an async call to the OpenAI API.\"\"\"\n    import openai\n    client = openai.AsyncOpenAI(api_key=settings.openai_api_key)"
new = "async def _call_openai(prompt: str, system: str | None, model: str, max_tokens: int, temperature: float) -> dict:\n    \"\"\"Make an async call to the OpenAI-compatible API (OpenAI or Groq).\"\"\"\n    import openai\n    groq_key = getattr(settings, \"groq_api_key\", \"\")\n    if groq_key and not settings.openai_api_key:\n        client = openai.AsyncOpenAI(api_key=groq_key, base_url=\"https://api.groq.com/openai/v1\")\n    else:\n        client = openai.AsyncOpenAI(api_key=settings.openai_api_key)"
content = content.replace(old, new)
with open("app/api/proxy.py", "w") as f:
    f.write(content)
print("Done!")
