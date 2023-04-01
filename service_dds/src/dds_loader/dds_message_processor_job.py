from datetime import datetime
import time
from logging import Logger
import json
import uuid
from time import localtime, strftime

from lib.pg.pg_connect import PgConnect
from lib.kafka_connect.kafka_connectors import KafkaProducer, KafkaConsumer
from dds_loader.repository.dds_repository import DdsRepository

from pydantic import BaseModel, ValidationError

class Validate_products(BaseModel):
    id: str
    price: float
    quantity: int
    name: str
    category: str

class Validate_user(BaseModel):
    id: str
    name: str
    login: str

class Validate_restaurant(BaseModel):
    id: str
    name: str

class Validate_payload(BaseModel):
    id: str
    date: datetime
    cost: float
    payment: float
    status: str
    restaurant: Validate_restaurant
    user: Validate_user
    products: list[Validate_products]

class Validate_h_restaurant(BaseModel):
    object_id: str
    object_type: str
    payload: Validate_payload

class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger

    def run(self) -> None:
        while True:
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: no message")
                continue
            
            if msg:
                self._logger.info(f"{datetime.utcnow()}: message read")

                json_data = json.dumps(msg)

                try:
                    valid_message = Validate_h_restaurant.parse_raw(json_data)
                except ValidationError as e:
                    self._logger.info(f"{datetime.utcnow()}: !!!!!!!!!!       E R R O R        !!!!!!!! {e.json}")
                else:
                    self._logger.info(f"{datetime.utcnow()}: !!!!!!!!!!       MESSAGE        {valid_message}")

                self._logger.info(f"{datetime.utcnow()}: h_restaurant insert")
                order = msg['payload']
                restaurant_id = order['restaurant']['id']
                restaurant_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), restaurant_id)
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.h_restaurant_insert(restaurant_uuid, restaurant_id, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: h_user insert")
                user_id = order['user']['id']
                user_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), user_id)
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.h_user_insert(user_uuid, user_id, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: h_order insert")
                order_id, order_dt = order['id'], order['date']
                order_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(order_id))
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.h_order_insert(order_uuid, int(order_id), order_dt, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: h_product insert")
                products = order['products']
                for product in products:
                    product_id = product['id']
                    product_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), product_id)
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._dds_repository.h_product_insert(product_uuid, product_id, current_datetime, 'stg-service-orders')

                    self._logger.info(f"{datetime.utcnow()}: h_category insert")
                    category_name = product['category']
                    category_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), category_name)
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._dds_repository.h_category_insert(category_uuid, category_name, current_datetime, 'stg-service-orders')

                    self._logger.info(f"{datetime.utcnow()}: l_order_product insert")
                    hk_order_product_pk = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(str(order_uuid)+str(product_uuid)))
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._dds_repository.l_order_product_insert(hk_order_product_pk, order_uuid, product_uuid, current_datetime, 'stg-service-orders')
                    
                    self._logger.info(f"{datetime.utcnow()}: l_product_category insert")
                    hk_product_category_pk = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(str(product_uuid) + str(category_uuid)))
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._dds_repository.l_product_category_insert(hk_product_category_pk, product_uuid, category_uuid, current_datetime, 'stg-service-orders')

                    self._logger.info(f"{datetime.utcnow()}: l_product_restaurant insert")  
                    hk_product_restaurant_pk = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(str(product_uuid) + str(restaurant_uuid)))
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._dds_repository.l_product_restaurant_insert(hk_product_restaurant_pk, product_uuid, restaurant_uuid, current_datetime, 'stg-service-orders')

                    self._logger.info(f"{datetime.utcnow()}: s_product_names insert")  
                    product_uuid2 = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(product_uuid))
                    product_name = product['name']
                    current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                    self._dds_repository.s_product_names_insert(product_uuid2, product_uuid, product_name, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}. Sending topic dds-service-orders")
                dst_msg = {
                    "id": msg["object_id"],
                    "user": str(user_uuid),
                    "products": self._format_items(msg['payload']['products'])
                    }
                
                self._producer.produce(dst_msg)

                self._logger.info(f"{datetime.utcnow()}: l_order_user insert")  
                hk_order_user_pk = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(str(order_uuid)+str(user_uuid)))
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.l_order_user_insert(hk_order_user_pk, order_uuid, user_uuid, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: s_order_cost insert")  
                order_uuid2 = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(order_uuid))
                cost, payment = order['cost'], order['payment']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.s_order_cost_insert(order_uuid2, order_uuid, float(cost), float(payment), current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: s_order_status insert")  
                status = order['status']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.s_order_status_insert(order_uuid2, order_uuid, status, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: s_restaurant_names insert")  
                restaurant_uuid2 = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(restaurant_uuid))
                rest_name = order['restaurant']['name']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.s_restaurant_names_insert(restaurant_uuid2, restaurant_uuid, rest_name, current_datetime, 'stg-service-orders')

                self._logger.info(f"{datetime.utcnow()}: s_user_names insert")  
                user_uuid2 = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), str(user_uuid))
                user_name, user_login = order['user']['name'], order['user']['login']
                current_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
                self._dds_repository.s_user_names_insert(user_uuid2, user_uuid, user_name, user_login, current_datetime, 'stg-service-orders')

    def _format_items(self, products):
        items = []

        for product in products:
            product_id = product['id']
            product_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), product_id)
            product_name = product['name']
            category_name = product['category']
            category_uuid = uuid.uuid3(getattr(uuid, 'NAMESPACE_OID'), category_name)
            dst_it = {
                "product_uuid": str(product_uuid),
                "product_name": product_name,
                "category_uuid": str(category_uuid),
                "category_name": category_name
            }
            items.append(dst_it)

        return items