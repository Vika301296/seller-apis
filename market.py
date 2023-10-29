import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получить список товаров компании на Яндекс.Маркет.

    Args:
        page (str): Токен страницы для запроса.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа.

    Returns:
        list: Список товаров компании.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Примеры:
        >>> get_product_list("page123", "campaign123", "access_token123")
        [{'item': 'product123', 'price': 100.0},
        {'item': 'product124', 'price': 150.0}]

        >>> get_product_list(123, "campaign123", "access_token123")
        TypeError: expected string or bytes-like object
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновить информацию об остатках товаров на Яндекс.Маркет.

    Args:
        stocks (list): Список информации об остатках товаров.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа.

    Returns:
        dict: Результат операции.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Examples:
        >>> update_stocks([{"stock": 5, "item_id": "123"}],
        >>>               "campaign123", "access_token123")
        {'result': 'success'}

        >>> update_stocks("5", "campaign123", "access_token123")
        TypeError: expected list
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновить цены на товары на Яндекс.Маркет.

    Args:
        prices (list): Список цен на товары.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа.

    Returns:
        dict: Результат операции.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Примеры:
        >>> update_price([{"price": "5990", "item_id": "123"}],
        >>>              "campaign123", "access_token123")
        {'result': 'success'}

        >>> update_price("5990", "campaign123", "access_token123")
        TypeError: expected list
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получить артикулы товаров на Яндекс.Маркете.

    Args:
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен доступа.

    Returns:
        list: Список артикулов товаров.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Примеры:
        >>> get_offer_ids("campaign123", "market_token123")
        ['offer123', 'offer124']

        >>> get_offer_ids("campaign123", 123)
        TypeError: expected string or bytes-like object
    """

    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создать список информации об остатках товаров на Яндекс.Маркете.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.
        warehouse_id (str): Идентификатор склада.

    Returns:
        list: Список информации об остатках товаров.

    Примеры:
        >>> create_stocks([{"Код": "123", "Количество": ">10"}],
        >>>               ["123"], "warehouse123")
        [
            {'offer_id': '123', 'stock': 100}
        ]

        >>> create_stocks([{"Код": "123", "Количество": ">10"}],
        >>                "invalid", "warehouse123")
        TypeError: expected list
    """

    # Уберем то, что не загружено в market
    stocks = list()
    date = str(
        datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создать список цен на товары на Яндекс.Маркете.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список цен на товары.

    Примеры:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], ["123"])
        [{
            'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
            'offer_id': '123', 'old_price': '0', 'price': '5990'
        }]

        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], "invalid")
        TypeError: expected list
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загрузить цены на товары на Яндекс.Маркет.

    Args:
        watch_remnants (list[dict]): Список остатков товаров
        с информацией о ценах.
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен доступа.

    Returns:
        list: Список цен на товары с указанием валюты,
        загруженных на Яндекс.Маркет.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Examples:
        >>> upload_prices([{"Код": "123", "Цена": "5'990.00 руб."}],
                          "campaign123", "market_token123")
        [{
            'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
            'offer_id': '123', 'old_price': '0', 'price': '5990'
        }]

        >>> await upload_prices([{"Код": "123", "Price": "5'990.00 руб."}],
                                123, 456)
        # Вы можете увидеть ошибку BadRequest в терминале.

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(
        watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Загрузить информацию об остатках товаров на Яндекс.Маркет.

    Args:
        watch_remnants (list): Список остатков товаров.
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен доступа.
        warehouse_id (str): Идентификатор склада.

    Returns:
        tuple: Две части - не пустые остатки и все остатки.

    Examples:
        >>> upload_stocks([{"Код": "123", "Количество": "5"}], "campaign123",
        >>>     "market_token123", "warehouse123")
        ([
            {'offer_id': '123', 'stock': 5}
        ], [
            {'offer_id': '123', 'stock': 5}
        ])

        >>> upload_stocks([{"Code": "123", "Stock": "5"}], 123, 456,
        >>>     "warehouse123")
        # Вы можете увидеть ошибку BadRequest в терминале.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
