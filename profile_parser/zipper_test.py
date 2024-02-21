import os
import zipfile
import rarfile
import json
import datetime


def zipper_converter():
    archive_path = "archive_detection"
    data_path = "test_data"

    if not os.path.exists(data_path):
        os.mkdir(data_path)

    for file in os.listdir(archive_path):
        file_path = os.path.join(archive_path, file)
        if zipfile.is_zipfile(file_path) or rarfile.is_rarfile(file_path):
            if zipfile.is_zipfile(file_path):
                archive = zipfile.ZipFile(file_path, "r")
            else:
                archive = rarfile.RarFile(file_path, "r")
            for name in archive.namelist():
                if name.endswith(".json"):
                    archive.extract(name, data_path)
                    new_name = file.split(".")[0] + "_" + name
                    # new_name = file.split(".")[0] + "_" + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + "_" + name
                    os.rename(os.path.join(data_path, name), os.path.join(data_path, new_name))
            archive.close()


def main():
    zipper_converter()


if __name__ == '__main__':
    main()
