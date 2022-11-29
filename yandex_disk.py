from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import logging
from pathlib import Path

import yadisk

with open('settings.json', 'r') as f:
    settings = json.load(f)
    FOLDER_RECORDS = settings['path_to_records']
    TOKEN, ROTATION_DAYS = settings['yandex'].values()


def upload_videos(y: yadisk.YaDisk, dt: datetime):
    """ Upload files to Yandex cloud without file extension,
     move extension back, delete local files and check
     if files on cloud are older than ROTATION_DAYS
     """

    date_dir = Path(FOLDER_RECORDS, dt.strftime('%d-%m-%y'))
    for camera_dir in date_dir.iterdir():
        if camera_dir.is_dir():
            hour_dir = Path(camera_dir, dt.strftime('%Hh'))
            filepaths = [f for f in hour_dir.iterdir()]
            assert len(filepaths) == 1
            filepath = filepaths[0]
            # files with extension uploads to cloud very slowly
            cloud_filepath = Path(camera_dir, filepath.stem)

            for _dir in reversed(cloud_filepath.parents[:-1]):
                try:
                    y.mkdir(_dir)
                except yadisk.exceptions.PathExistsError:
                    pass

            # y.upload(str(filepath), str(cloud_filepath), timeout=(10, 10*60))
            # # extension coming back
            # y.move(str(cloud_filepath), str(cloud_filepath.with_suffix(filepath.suffix)))

            filepath.unlink()
            hour_dir.rmdir()
            try:
                camera_dir.rmdir()
            except OSError:
                pass

            logging.info(f'{filepath.name} was uploaded and removed from local')
    else:
        logging.info(f'All videos by {dt.strftime("%d-%m-%y")} date and {dt.strftime("%H")} hour'
                     f' was uploaded and removed from local')

    try:
        date_dir.rmdir()
    except OSError:
        pass


def remove_old_streams(y: yadisk.YaDisk, dt: datetime):
    """ ROTATIONS_DAYS from settings.json using here to remove old files from cloud """

    date_until_remove = dt - timedelta(days=int(ROTATION_DAYS))
    for dir_data in y.listdir(f'/{FOLDER_RECORDS}'):
        dir_date = datetime.strptime(dir_data['name'], '%d-%m-%y').replace(tzinfo=ZoneInfo('Europe/Moscow'))
        if dir_date < date_until_remove:
            y.remove(
                str(Path(FOLDER_RECORDS, dir_data["name"])),
                permanently=True,
                timeout=60
            )
            logging.info(f'Folder "{dir_data["name"]}" was removed from yandex disk')


def upload_and_remove(dt):
    y = yadisk.YaDisk(token=TOKEN)
    upload_videos(y, dt)
    remove_old_streams(y, dt)

