from db.models import *
import datetime

from parser.worker import Worker
from settings import ACCESS_TOKEN

if __name__ == '__main__':
    try:
        data = {
            'район Выхино-Жулебино': [
                'vykhino_zhulebino_online',
                # 'vyhino_zhulebino_online',
                # 'uvao_vihino',
                # 'vihino_julebino_nekrasovka'
            ],
            'Академический район': [
                'akademicheskiy_online',
                # 'akadem_raion'
            ],
            'Алексеевский район': [
                'upravaalexeevsky'
            ]
        }

        worker = Worker(ACCESS_TOKEN, data)
        worker.start_worker()
    except ValueError as e:
        print(e)
