import logging
import os.path
import re
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from multiprocessing import Process

from requests import Session, RequestException
import cv2

from my_logging import get_logger
from my_threading import MyThread
from yandex_disk import upload_and_remove


with open('settings.json', 'r') as f:
    PIK_LOGIN, PIK_PASSWORD, CAMERAS_NAMES = json.load(f)['pik'].values()

DEFAULT_DEVICE_ID = 'F03C07A2-3C00-4555-823A-D7F2DC854A7C'
DEFAULT_USER_AGENT = 'domophone-ios/315645 CFNetwork/1390 Darwin/22.0.0'

VIDEO_EXTENSION = '.avi'
FPS = 25
RESOLUTION = (1280, 720)

FOLDER_STREAM_PARTS = 'stream_parts'
FOLDER_STREAM_TO_UPLOAD = 'stream_to_upload'
_ = [os.mkdir(folder) for folder in (
        FOLDER_STREAM_PARTS,
        FOLDER_STREAM_TO_UPLOAD
    )
    if not os.path.exists(folder)]


class IntercomRecorder:
    def __init__(self):
        self.phone_number = PIK_LOGIN
        self.password = PIK_PASSWORD
        self.device_id = DEFAULT_DEVICE_ID
        self.user_agent = DEFAULT_USER_AGENT
        self.record_threads = None

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
        else:
            raise RequestException('Authorization error')

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
                    'name': d['relays'][0]['name'],
                    'url': d['relays'][0]['rtsp_url'],
                    'id': d['relays'][0]['id']
                }
                for d in r.json() if d['relays'][0]['rtsp_url'] and d['relays'][0]['name'].lower() in CAMERAS_NAMES
            ]
        else:
            raise RequestException('Get stream urls error')

    def record_stream(self, stream_data: dict) -> Path:
        """ Capturing RTSP stream """

        def get_stream_filepath(name: str) -> Path:
            current_datetime = datetime.now(tz=ZoneInfo('Europe/Moscow'))
            filename = '_'.join(f"{name}_{current_datetime.strftime('%d-%m-%y %Hh')}_1".split())
            while True:
                if filename + VIDEO_EXTENSION not in os.listdir(FOLDER_STREAM_PARTS):
                    return Path(FOLDER_STREAM_PARTS, filename + VIDEO_EXTENSION)
                i = re.search(r'(?<=_)\d+$', filename).group(0)
                filename = f"{filename[:-len(i)]}{int(i) + 1}"

        filepath = get_stream_filepath(stream_data['name'])
        ms = f'name="{filepath.name}". '
        logging.info(f'{ms}Start capture stream')

        out = cv2.VideoWriter(str(filepath), cv2.VideoWriter_fourcc(*'XVID'), FPS, RESOLUTION)
        cap = cv2.VideoCapture(stream_data['url'])
        try:
            while cap.isOpened() and all(tr.is_alive() for tr in self.record_threads):
                ret, frame = cap.read()
                if not ret or datetime.now().minute == 0 and datetime.now().second == 0:
                    logging.info(f"{ms}[Stream ended] Can't receive frame.")
                    break
                out.write(frame)
                time.sleep(.01)
            else:
                logging.info(f'{ms}Capture is closed')
        finally:
            cap.release()
            out.release()

        return filepath

    @staticmethod
    def concatenate_video_parts(video_paths: list[Path]):
        """ Concatenate 2.5 videos to n minutes one video """
        video_paths.sort(key=lambda x: int(re.search(r'(?<=_)\d+$', '.'.join(str(x).split('.')[:-1])).group(0)))
        new_filename = '_'.join(video_paths[0].stem.split('_')[:-1])
        new_filepath = Path(FOLDER_STREAM_TO_UPLOAD, *video_paths[0].parts[1:-1], new_filename + VIDEO_EXTENSION)
        logging.info(f'Start concatenate captured videos for {new_filename}')

        # Create new video with writer from all filepaths
        out = cv2.VideoWriter(str(new_filepath), cv2.VideoWriter_fourcc(*'XVID'), FPS, RESOLUTION)
        for video_path in video_paths:
            logging.info(f'Concatenating {video_path.name}')
            video = cv2.VideoCapture(str(video_path))
            while video.isOpened():
                r, frame = video.read()
                if not r:
                    break
                out.write(frame)
            video.release()
            os.remove(video_path)
        out.release()

        logging.info(f'{new_filename} concatenated')
        return new_filepath

    @staticmethod
    def wait_concat_and_upload(prs: list[Process], date: str):
        for pr in prs: pr.join()
        upload_and_remove(local_folder=FOLDER_STREAM_TO_UPLOAD, date=date)

    def start_recording(self):
        while True:
            with Session() as s:
                try:
                    bearer_token = self.authorize(s)
                except RequestException as error:
                    logging.error(error)
                    continue

                videos_uploaded = False
                while True:
                    try:
                        streams_data = self.get_available_streams(s, bearer_token)
                    except RequestException as error:
                        logging.error(error)
                        break

                    self.record_threads = [MyThread(
                        target=self.record_stream,
                        args=(stream_data,),
                    )
                        for stream_data in streams_data]

                    for tr in self.record_threads: tr.start()
                    for tr in self.record_threads: tr.join()

                    current_dt = datetime.now(tz=ZoneInfo('Europe/Moscow'))
                    if not videos_uploaded and current_dt.minute == 0:
                        prs_concat = []
                        for stream_data in streams_data:
                            filepaths_to_concat = [
                                Path(FOLDER_STREAM_PARTS, filename)
                                for filename in os.listdir(FOLDER_STREAM_PARTS)
                                if f"{'_'.join(stream_data['name'].split())}" in filename and \
                                str(current_dt.hour - 1).zfill(2) + 'h' in filename
                            ]
                            pr = Process(
                                target=self.concatenate_video_parts,
                                args=(filepaths_to_concat,)
                            )
                            prs_concat.append(pr)
                            pr.start()

                        videos_uploaded = True
                        MyThread(
                            target=self.wait_concat_and_upload,
                            args=(
                                prs_concat,
                                current_dt.strftime('%d-%m-%y')
                            )
                        ).start()
                    elif videos_uploaded and current_dt.minute >= 15:
                        videos_uploaded = False


if __name__ == '__main__':
    get_logger('IntercomRecorder.log')
    IntercomRecorder().start_recording()
