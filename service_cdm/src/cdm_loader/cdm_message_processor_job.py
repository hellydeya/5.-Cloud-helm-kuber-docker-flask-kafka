from datetime import datetime
from logging import Logger
from uuid import UUID
import pandas as pd
import json

from lib.kafka_connect import KafkaConsumer
from lib.pg.pg_connect import PgConnect
from cdm_loader.repository.cdm_insert import CdmInsert

class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                cdm_insert: CdmInsert,
                logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._cdm_insert = cdm_insert
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        while True:
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: no message")
                continue
            
            if msg:
                self._cdm_insert.cdm_up(list(map(tuple, pd.json_normalize(msg, meta=['id', 'user'], record_path='products', errors='ignore').groupby(['user', 'product_uuid', 'product_name']).agg(order_cnt = ('id', 'nunique')).reset_index().values)))

                self._cdm_insert.cdm_uc(list(map(tuple, pd.json_normalize(msg, meta=['id', 'user'], record_path='products', errors='ignore').groupby(['user', 'category_uuid', 'category_name']).agg(order_cnt = ('id', 'nunique')).reset_index().values)))

