import logging
import re
import json
import subprocess
import time
import argparse
import shutil
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from threading import Thread

from requests import Session, RequestException

from my_logging import get_logger
from yandex_disk import upload_and_remove


with open('settings.json', 'r') as f:
    settings = json.load(f)
    FOLDER_RECORDS = settings['path_to_records']
    PIK_LOGIN, PIK_PASSWORD, CAMERAS_NAMES = settings['pik'].values()

DEFAULT_DEVICE_ID = 'NKGHS3I6-3C00-4555-823A-D7F2DC854A7C'
DEFAULT_USER_AGENT = 'domophone-ios/315645 CFNetwork/1390 Darwin/22.0.0'

EXTENSION = '.mp4'


class IntercomRecorder:
    def __init__(self, pik_login=PIK_LOGIN, pik_password=PIK_PASSWORD,
                 device_id=DEFAULT_DEVICE_ID, user_agent=DEFAULT_USER_AGENT):
        self.phone_number = pik_login
        self.password = pik_password
        self.device_id = device_id
        self.user_agent = user_agent
        self.tr_concat = None
        self.tr_upload = None

    def authorize(self, s: Session) -> str:
        headers = {
            'Host': 'intercom.pik-comfort.ru',
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
            'User-Agent': self.user_agent,
            'Api-Version': '2',
            'Accept-Language': 'ru',
            'Content-Length': '128',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'close',
        }
        data = {
            'account[password]': self.password,
            'account[phone]': self.phone_number,
            'customer_device[uid]': self.device_id,
        }
        r = s.post(
            'https://intercom.pik-comfort.ru/api/customers/sign_in',
            headers=headers,
            data=data,
            timeout=10
        )
        if r.status_code == 200:
            logging.info('Authorized to pik intercom')
            return r.headers['Authorization']
        raise RequestException(f'Authorization error. Response code is {r.status_code}')

    def get_available_streams(self, s: Session, bearer_token: str) -> list[dict]:
        """ Get available name and url for every stream """
        headers = {
            'Host': 'iot.rubetek.com',
            'Accept': '*/*',
            'Authorization': bearer_token,
            'Device-Client-App': 'alfred',
            'Api-Version': '2',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru',
            'Device-Client-Uid': self.device_id,
            'User-Agent': self.user_agent,
            'Device-Client-Version': '2022.8.1',
            'Device-Client-Os': 'iOS',
        }

        params = {
            'page': '1',
        }

        r = s.get(
            'https://iot.rubetek.com/api/alfred/v1/personal/intercoms',
            params=params,
            headers=headers,
            timeout=10
        )
        if r.status_code != 200:
            raise RequestException(f'Get stream urls error. Response code is {r.status_code}')

        streams_data = []
        for d in r.json():
            for relay in d['relays']:
                if relay['rtsp_url'] and relay['name'].lower() in CAMERAS_NAMES:
                    stream_data = {
                        'name': '_'.join(relay['name'].split()),
                        'url': relay['rtsp_url'],
                        'id': relay['id']
                    }
                    streams_data.append(stream_data)

        for camera_name in set(['_'.join(x.split()) for x in CAMERAS_NAMES]).\
                difference([d['name'].lower() for d in streams_data]):
            logging.warning(f'Did not get stream data for camera "{camera_name}"')
        return streams_data

    @staticmethod
    def get_stream_filepath(camera_name: str) -> Path:
        """ return FOLDER_RECORDS/camera_name/date/hour/[camera_name]_[date]_[hour]h_[n].[extension] """

        current_dt = datetime.now(tz=ZoneInfo('Europe/Moscow'))
        filepath = Path(
            FOLDER_RECORDS,
            current_dt.strftime('%d-%m-%y'),
            camera_name,
            current_dt.strftime('%Hh'),
            f"{camera_name}_{current_dt.strftime('%d-%m-%y_%Hh')}_1"
        ).with_suffix(EXTENSION)

        for p in reversed(filepath.parents):
            p.mkdir(exist_ok=True)

        if list(filepath.parent.iterdir()):
            parts = [int(re.search(r'(?<=_)\d+$', p.stem).group(0)) for p in filepath.parent.iterdir()]
            parts.sort()
            filepath = Path(
                filepath.parent,
                re.sub(r'(?<=_)\d+$', str(parts[-1] + 1), filepath.stem)
            ).with_suffix(EXTENSION)
        return filepath

    def record_all_streams(self, streams_data: list[dict]) -> None:
        """ Capturing RTSP stream """

        prs = []
        for stream_data in streams_data:
            filepath = self.get_stream_filepath(stream_data['name'])
            logging.info(f'START capture stream. name="{filepath.name}"')

            pr = subprocess.Popen(
                [
                    'ffmpeg',
                    '-rtsp_transport', 'tcp',
                    '-i', stream_data['url'],
                    '-q:v', '1',  # quality parameter
                    '-t', '00:02:00',
                    '-loglevel', 'fatal',
                    filepath,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE
            )
            prs.append((pr, filepath))

        while True:
            for pr_data in prs:
                if pr_data[0].poll() is not None:
                    logging.info(f'END capture stream. name={pr_data[1].name}')
                    prs.remove(pr_data)
            if len(prs) == 0:
                logging.info('END capture ALL streams')
                break
            time.sleep(.01)

        for pr_data in prs:
            try:
                err = pr_data[0].stderr.read().decode('utf-8')
                if err:
                    logging.error(err)
            except Exception as ex:
                logging.error(ex, exc_info=True)

    @staticmethod
    def fix_timestamp(filepath: Path):
        temp_filepath = Path(filepath.parent, filepath.stem + '_temp').with_suffix(filepath.suffix).absolute()

        pr = subprocess.run(
            [
                'ffmpeg',
                '-hide_banner',
                '-y',
                '-i', str(filepath),
                '-avoid_negative_ts', 'make_zero',
                '-video_track_timescale', '90000',
                '-c', 'copy',
                str(temp_filepath)
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        stderr = pr.stderr.decode('utf-8')

        if temp_filepath.exists():
            temp_filepath.replace(filepath)
        else:
            if re.search(
                    r'Output file #\d* does not contain any stream|'
                    r'Invalid data found when processing input',
                    stderr
            ):
                logging.warning(f'Remove {filepath.name}\n{stderr}')
                filepath.unlink()
            else:
                logging.error(f'UNUSUAL ERROR WHILE FIXING TIMESTAMP\n{stderr}')

    @staticmethod
    def concat_all_parts(dt: datetime) -> None:
        """ Concatenate n videos to 60 minutes video for every stream """

        def get_sorted_parts(dir_: Path) -> list[Path]:
            return sorted(
                [p.absolute() for p in dir_.iterdir() if re.search(r'(?<=_)\d+$', p.stem)],
                key=lambda p: int(re.search(r'(?<=_)\d+$', p.stem).group(0))
            )

        date_dir = Path(FOLDER_RECORDS, dt.strftime('%d-%m-%y'))
        for camera_dir in date_dir.iterdir():
            dir_parts = Path(camera_dir, dt.strftime('%Hh'))

            for filepath in get_sorted_parts(dir_parts):
                IntercomRecorder.fix_timestamp(filepath)

            parts_filepaths = get_sorted_parts(dir_parts)
            with open('list_video.txt', 'w') as f:
                f.write(''.join([f"file '{str(p)}'\n" for p in parts_filepaths]))

            new_filepath = Path(
                dir_parts, re.sub(r'_\d+$', '', parts_filepaths[0].stem)
            ).with_suffix(EXTENSION)

            logging.info(f'START concatenating {new_filepath.name}')
            pr = subprocess.run(
                [
                    'ffmpeg',
                    '-safe', '0',
                    '-f', 'concat',
                    '-i', 'list_video.txt',
                    '-c', 'copy',
                    new_filepath.absolute()
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

            stderr = pr.stderr.decode('utf-8')
            if new_filepath.exists():
                logging.info(f'END with SUCCESS concatenating {new_filepath.name}')
                for filepath in get_sorted_parts(dir_parts):
                    filepath.unlink()
                else:
                    logging.info(f'Local parts for {new_filepath.name} removed')
            else:
                logging.error(f'END with ERROR concatenating {new_filepath.name}\n{stderr}')
        else:
            logging.info(f'ALL parts concatenating tasks finished somehow')

    def wait_and_upload(self, dt: datetime):
        self.tr_concat.join()
        self.tr_concat = None

        try:
            upload_and_remove(dt)
        except Exception as ex:
            logging.error(ex, exc_info=True)
        finally:
            self.tr_upload = None

    @staticmethod
    def remove_local_dir_records():
        shutil.rmtree(FOLDER_RECORDS, ignore_errors=True)
        logging.info(f'Local directory {FOLDER_RECORDS}" was removed')

    def start_work(self):

        while True:
            with Session() as s:
                try:
                    bearer_token = self.authorize(s)
                except RequestException as ex:
                    logging.error(ex, exc_info=True)
                    continue

                while True:
                    dt_before = datetime.now(tz=ZoneInfo('Europe/Moscow'))

                    try:
                        streams_data = self.get_available_streams(s, bearer_token)
                    except RequestException as ex:
                        logging.error(ex, exc_info=True)
                        break

                    self.record_all_streams(streams_data)
                    current_dt = datetime.now(tz=ZoneInfo('Europe/Moscow'))

                    if not self.tr_upload \
                            and not self.tr_concat \
                            and dt_before.hour != current_dt.hour:

                        self.tr_concat = Thread(
                            target=self.concat_all_parts,
                            args=(dt_before,)
                        )
                        self.tr_concat.start()

                        self.tr_upload = Thread(
                            target=self.wait_and_upload,
                            args=(dt_before,)
                        )
                        self.tr_upload.start()


def __test_concat__():
    get_logger('test.log')
    streams_data = [
        # {'name': 'Подъезд_Вход_со_двора'},
        # {'name': 'Подъезд_Вход_с_улицы'},
        {'name': '13_Этаж_Дверь_1'}
    ]
    dt = datetime.now(tz=ZoneInfo('Europe/Moscow')) - timedelta(hours=17)
    IntercomRecorder.concat_all_parts(dt)


def main():
    intercom_recorder = IntercomRecorder()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r',
        action='store_true',
        help=f'before start run: rm -rf "{FOLDER_RECORDS}"'
    )
    args = parser.parse_args()

    if args.r:
        intercom_recorder.remove_local_dir_records()

    intercom_recorder.start_work()


if __name__ == '__main__':
    get_logger('IntercomRecorder.log')
    main()
