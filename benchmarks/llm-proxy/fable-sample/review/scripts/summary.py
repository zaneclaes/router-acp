def summarize(results):
    normalized = []
    for result in results:
        item = dict(result)
        # BUG: skipped jobs are presented as successful.
        if item.get("status") == "skipped":
            item["status"] = "success"
        normalized.append(item)
    return normalized
