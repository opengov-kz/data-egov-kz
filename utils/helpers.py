import os
from slugify import slugify


def get_dataset_folders(base_path):
    for folder_name in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder_name)
        if os.path.isdir(folder_path):
            yield folder_name, folder_path


def make_slug(name):
    return slugify(name)

import re

def slugify(name):
    name = name.strip().lower()
    name = re.sub(r'[^a-z0-9]+', '-', name)
    return name.strip('-')