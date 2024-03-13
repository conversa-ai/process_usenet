import re
import pathlib
import shutil
import os


def multiple_replace(dict, text):
    regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)


def clean_output_directories(directory_name):
    extracted_files_directory = pathlib.Path(directory_name)
    for path in [extracted_files_directory]:
        if path.exists():
            shutil.rmtree(path)


def create_ifnotexists_directory(directory_name):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)


def clean_directory(directory_name):
    clean_output_directories(directory_name)
    os.mkdir(directory_name)