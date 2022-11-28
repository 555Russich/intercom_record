from datetime import datetime, timedelta
import json
import os
import logging

import yadisk

with open('settings.json', 'r') as f:
    TOKEN, YANDEX_FOLDER_STREAMS, ROTATION_DAYS = json.load(f)['yandex'].values()


def upload_videos(y: yadisk.YaDisk, local_folder: str, date: str):
    try:
        y.mkdir(YANDEX_FOLDER_STREAMS)
    except yadisk.exceptions.PathExistsError:
        pass
    
    try:
        y.mkdir(f'{YANDEX_FOLDER_STREAMS}/{date}')
    except yadisk.exceptions.PathExistsError:
        pass
    
    for filename in os.listdir(local_folder):
        # files with .avi extension uploads to cloud very slowly, here extension removing
        filename, extension = '.'.join(filename.split('.')[:-1]), filename.split('.')[-1]
        local_path = f'{local_folder}/{filename}.{extension}'
        yandex_path = f'{YANDEX_FOLDER_STREAMS}/{date}/{filename}'
        y.upload(local_path, yandex_path, timeout=(10, 5*60))
        os.remove(local_path)
        # extension coming back
        y.move(yandex_path, f'{yandex_path}.{extension}')
        logging.info(f'{local_path} was uploaded and removed from local folder')
    else:
        logging.info(f'All files from {local_folder=} was uploaded and removed from local folder')


def remove_old_streams(y: yadisk.YaDisk, date: str):
    current_date = datetime.strptime(date, '%d-%m-%y')
    date_until_remove = current_date - timedelta(days=int(ROTATION_DAYS))
    
    for folder_data in y.listdir(f'/{YANDEX_FOLDER_STREAMS}'):   
        folder_date = datetime.strptime(folder_data['name'], '%d-%m-%y')
        if folder_date < date_until_remove:
            y.remove(
                f'/{YANDEX_FOLDER_STREAMS}/{folder_data["name"]}',
                permanently=True,
                timeout=60
            )
            logging.info(f'Folder "{folder_data["name"]}" was removed from yandex disk')


def upload_and_remove(local_folder: str, date: str):
    y = yadisk.YaDisk(token=TOKEN)
    upload_videos(y, local_folder, date)
    remove_old_streams(y, date)
