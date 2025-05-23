def transform(data):
    cleaned = []
    for row in data:
        if None in row.values():
            continue
        cleaned.append(row)
    return cleaned