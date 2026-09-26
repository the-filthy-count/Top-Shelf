def best_image_url(images: list) -> str | None:
    valid = [i for i in (images or []) if i.get("url")]
    if not valid:
        return None
    return max(valid, key=lambda i: (i.get("width") or 0) * (i.get("height") or 0))["url"]
