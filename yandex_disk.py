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
        local_path = f'{local_folder}/{filename}'
        filename, extension = '.'.join(filename.split('.')[:-1]), filename.split('.')[-1]
        yandex_path = f'{YANDEX_FOLDER_STREAMS}/{date}/{filename}'
        y.upload(local_path, yandex_path, timeout=(10, 3*60))
        os.remove(local_path)
        y.move(yandex_path, yandex_path + extension)
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
