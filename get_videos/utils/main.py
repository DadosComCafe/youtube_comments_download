def chunkify(array, chunk_size):
    """Yield successive n-sized chunks from l."""
    if not array:
        return array

    for i in range(0, len(array), chunk_size):
        yield array[i : i + chunk_size]
