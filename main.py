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
        if r.status_code == 200:
            return [
                {
                    'name': '_'.join(d['relays'][0]['name'].split()),
                    'url': d['relays'][0]['rtsp_url'],
                    'id': d['relays'][0]['id']
                }
                for d in r.json() if d['relays'][0]['rtsp_url'] and d['relays'][0]['name'].lower() in CAMERAS_NAMES
            ]
        raise RequestException(f'Get stream urls error. Response code is {r.status_code}')

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

        if [p for p in filepath.parent.iterdir()]:
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
                    '-loglevel', 'fatal',
                    self.get_stream_filepath(stream_data['name']),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE
            )
            prs.append(pr)

        while all(pr.poll() is None for pr in prs):
            if datetime.now().minute == 0 and datetime.now().second == 0:
                # for pr in prs:
                #     pr.terminate()
                break
            time.sleep(.01)
        logging.info(f'END capture ALL streams')

        for pr in prs:
            err = pr.stderr.read().decode('utf-8')
            if err:
                logging.error(err)


    @staticmethod
    def concat_all_parts(streams_data: list[dict], dt: datetime) -> None:
        """ Concatenate n videos to 60 minutes video for every stream """

        for stream_data in streams_data:
            dir_parts = Path(
                FOLDER_RECORDS, dt.strftime('%d-%m-%y'),
                stream_data['name'], dt.strftime('%Hh')
            )

            video_stems = [f.stem for f in dir_parts.iterdir()]
            video_stems.sort(key=lambda x: int(re.search(r'(?<=_)\d+$', x).group(0)))
            parts_filepaths = [Path(dir_parts, stem).with_suffix(EXTENSION).absolute()
                               for stem in video_stems]

            new_filepath = Path(
                dir_parts, re.sub(r'_\d+$', '', video_stems[0])
            ).with_suffix(EXTENSION)

            logging.info(f'START concatenating {new_filepath.name}')
            subprocess.run(
                [
                    'ffmpeg',
                    '-safe', '0',
                    '-f', 'concat',
                    # '-segment_time_metadata', '1'
                    '-i', '/dev/stdin',
                    '-c', 'copy',
                    # '-vf', 'select=concatdec_select',
                    # '-af', 'aselect=concatdec_select,aresample=async=1',
                    new_filepath.absolute()
                ],
                input="".join(f'file {filepath}\n' for filepath in parts_filepaths),
                text=True
            )
            logging.info(f'END concatenating {new_filepath.name}')
            # for filepath in parts_filepaths:
            #     filepath.unlink()
        else:
            logging.info(f'ALL parts for all stream was concatenated. Local files was removed')

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
                except RequestException as error:
                    logging.error(error)
                    continue

                while True:
                    try:
                        streams_data = self.get_available_streams(s, bearer_token)
                    except RequestException as error:
                        logging.error(error)
                        break

                    self.record_all_streams(streams_data)

                    current_dt = datetime.now(tz=ZoneInfo('Europe/Moscow'))
                    if not self.tr_upload and not self.tr_concat and current_dt.minute == 0:
                        dt_to_concat = current_dt - timedelta(hours=1)

                        self.tr_concat = Thread(
                            target=self.concat_all_parts,
                            args=(streams_data, dt_to_concat)
                        )
                        self.tr_concat.start()

                        self.tr_upload = Thread(
                            target=self.wait_and_upload,
                            args=(dt_to_concat,)
                        )
                        self.tr_upload.start()


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
    # streams_data = [
    #     # {'name': 'Подъезд_Вход_со_двора'},
    #     # {'name': 'Подъезд_Вход_с_улицы'},
    #     {'name': '13_Этаж_Дверь_1'}
    # ]
    # dt = datetime.now(tz=ZoneInfo('Europe/Moscow')) - timedelta(hours=0)
    # IntercomRecorder.concat_all_parts(streams_data, dt)