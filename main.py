import logging
import re
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from multiprocessing import Process

from requests import Session, RequestException
import cv2

from my_logging import get_logger
from my_threading import MyThread
from yandex_disk import upload_and_remove


with open('settings.json', 'r') as f:
    settings = json.load(f)
    FOLDER_RECORDS = settings['path_to_records']
    PIK_LOGIN, PIK_PASSWORD, CAMERAS_NAMES = settings['pik'].values()

DEFAULT_DEVICE_ID = 'F03C07A2-3C00-4555-823A-D7F2DC854A7C'
DEFAULT_USER_AGENT = 'domophone-ios/315645 CFNetwork/1390 Darwin/22.0.0'

EXTENSION = '.avi'
FPS = 25
RESOLUTION = (1280, 720)


videos_uploaded = False


class IntercomRecorder:
    def __init__(self):
        self.phone_number: str = PIK_LOGIN
        self.password: str = PIK_PASSWORD
        self.device_id: str = DEFAULT_DEVICE_ID
        self.user_agent: str = DEFAULT_USER_AGENT
        self.record_threads: list = []

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

    def record_stream(self, stream_data: dict) -> Path:
        """ Capturing RTSP stream """

        def get_stream_filepath(camera_name: str) -> Path:
            """ mkdir FOLDER_RECORDS/date
                mkdir FOLDER_RECORDS/date/camera_name
                mkdir FOLDER_RECORDS/date/camera_name/hour
                :return: FOLDER_RECORDS/camera_name/date/hour/[camera_name]_[date]_[hour]H_[n].[extension]
            """

            current_dt = datetime.now(tz=ZoneInfo('Europe/Moscow'))
            filepath = Path(FOLDER_RECORDS,
                            current_dt.strftime('%d-%m-%y'),
                            camera_name,
                            current_dt.strftime('%Hh'),
                            f"{camera_name}_{current_dt.strftime('%d-%m-%y_%Hh')}_1"
                            ).with_suffix(EXTENSION)

            for p in reversed(filepath.parents):
                p.mkdir() if not p.exists() else None

            if [p for p in filepath.parent.iterdir()]:
                parts = [int(re.search(r'(?<=_)\d+$', p.stem).group(0)) for p in filepath.parent.iterdir()]
                parts.sort()
                filepath = Path(filepath.parent,
                                re.sub(r'(?<=_)\d+$', str(parts[-1] + 1), filepath.stem)
                                ).with_suffix(EXTENSION)
            return filepath

        filepath = get_stream_filepath(stream_data['name'])
        logging.info(f'name="{filepath.name}". START capture stream')

        out = cv2.VideoWriter(str(filepath), cv2.VideoWriter_fourcc(*'XVID'), FPS, RESOLUTION)
        cap = cv2.VideoCapture(stream_data['url'])
        try:
            while cap.isOpened() and all(tr.is_alive() for tr in self.record_threads):
                ret, frame = cap.read()
                if not ret or datetime.now().minute == 0 and datetime.now().second == 0:
                    logging.info(f'name="{filepath.name}" END capture stream')
                    break
                out.write(frame)
                time.sleep(.01)
            else:
                logging.info(f'name="{filepath.name}". Capture is closed. *probably was not even open')
        finally:
            cap.release()
            out.release()
        return filepath

    @staticmethod
    def concatenate_video_parts(camera_name: str, dt: datetime):
        """ Concatenate 2 videos to n minutes one video """

        dir_parts = Path(FOLDER_RECORDS,
                         dt.strftime('%d-%m-%y'),
                         camera_name,
                         dt.strftime('%Hh'),
                         )
        video_stems = [f.stem for f in dir_parts.iterdir()]
        video_stems.sort(key=lambda x: int(re.search(r'(?<=_)\d+$', x).group(0)))
        new_filepath = Path(dir_parts, re.sub(r'_\d+$', '', video_stems[0])).with_suffix(EXTENSION)
        logging.info(f'START concatenating {new_filepath.name}')

        # Concatenate all parts to create new large video
        out = cv2.VideoWriter(str(new_filepath), cv2.VideoWriter_fourcc(*'XVID'), FPS, RESOLUTION)
        for video_stem in video_stems:
            video_path = Path(dir_parts, video_stem).with_suffix(EXTENSION)
            logging.info(f'Concatenating {video_path.name}')
            video = cv2.VideoCapture(str(video_path))
            while video.isOpened():
                r, frame = video.read()
                if not r:
                    break
                out.write(frame)
            video.release()
            video_path.unlink()
        out.release()

        logging.info(f'END concatenating {new_filepath.name}')
        return new_filepath

    @staticmethod
    def wait_concat_and_upload(prs: list[Process], dt: datetime):
        global videos_uploaded

        for pr in prs:
            pr.join()

        upload_and_remove(dt)
        videos_uploaded = False

    def start_recording(self):
        global videos_uploaded

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

                    self.record_threads = [MyThread(
                        target=self.record_stream,
                        args=(stream_data,)
                    )
                        for stream_data in streams_data]

                    for tr in self.record_threads:
                        tr.start()
                    for tr in self.record_threads:
                        tr.join()

                    current_dt = datetime.now(tz=ZoneInfo('Europe/Moscow'))
                    if not videos_uploaded and current_dt.minute == 0:
                        prs_concat = []
                        dt_to_concat = current_dt - timedelta(hours=1)
                        for stream_data in streams_data:
                            pr = Process(
                                target=self.concatenate_video_parts,
                                args=(
                                    stream_data['name'],
                                    dt_to_concat
                                )
                            )
                            prs_concat.append(pr)
                            pr.start()

                        videos_uploaded = True
                        MyThread(
                            target=self.wait_concat_and_upload,
                            args=(
                                prs_concat,
                                dt_to_concat
                            )
                        ).start()


if __name__ == '__main__':
    get_logger('IntercomRecorder.log')
    IntercomRecorder().start_recording()
