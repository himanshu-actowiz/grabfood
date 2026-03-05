import json
import gzip
import os

def read_gzip_files(path: str, start: int, end: int):
    files = sorted(os.listdir(path)) 

    selected_files = files[start:end] 

    for file in selected_files:
        if file.endswith('.gz'):
            file_name = os.path.join(path, file)

            try:
                with gzip.open(file_name, 'rt', encoding='utf-8') as f:
                    yield json.load(f)
            except Exception as e:
                continue

