with open("workers/tasks.py", "r") as f:
    content = f.read()
old = "    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)\n    mlflow.set_experiment(\"llm-eval\")\n\n    with mlflow.start_run(run_name=f\"eval-{call_id[:8]}\") as run:"
new = "    run_id = None\n    try:\n        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)\n        mlflow.set_experiment(\"llm-eval\")\n        mlflow_ctx = mlflow.start_run(run_name=f\"eval-{call_id[:8]}\")\n        run = mlflow_ctx.__enter__()\n        run_id = run.info.run_id\n        mlflow.log_params({\"model\": model, \"call_id\": call_id, \"app_id\": app_id})\n        mlflow.log_metrics({\"faithfulness\": result.faithfulness, \"relevance\": result.relevance, \"toxicity\": result.toxicity, \"overall_score\": result.overall_score, \"golden_similarity\": golden_sim})\n        mlflow_ctx.__exit__(None, None, None)\n    except Exception as mlflow_err:\n        pass\n    if False:"
content = content.replace(old, new)
with open("workers/tasks.py", "w") as f:
    f.write(content)
print("Done!" if old not in open("workers/tasks.py").read() else "Pattern not found")
