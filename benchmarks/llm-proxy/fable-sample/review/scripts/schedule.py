def schedule(selected, manifest):
    services = manifest["services"]
    selected = [name for name in selected if name in services]
    # BUG: alphabetical order is not dependency order.
    return sorted(selected)


def services_for_paths(paths, manifest):
    matched = set()
    for name, config in manifest["services"].items():
        if any(path.startswith(prefix) for path in paths for prefix in config["paths"]):
            matched.add(name)
    # BUG: shared packages affect every service but match no service prefix.
    return sorted(matched)
