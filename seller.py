import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получить список товаров магазина Озон.

    Args:
        last_id (str): Идентификатор последнего полученного товара.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        list: Список товаров.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Examples:
        >>> get_product_list("last123", "client123", "token123")
        >>>     [{'item': 'product123', 'price': 100.0},
        {'item': 'product124', 'price': 150.0}]

        >>> get_product_list(123, "client123", "token123")
        # Вы можете увидеть ошибку BadRequest в терминале.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получить артикулы товаров магазина Озон.

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        list: Список артикулов товаров.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Examples:
        >>> get_offer_ids("client123", "token123")
        ['offer123', 'offer124']

        >>> get_offer_ids("client123", 123)
        TypeError: expected string or bytes-like object
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновить цены товаров.

    Args:
        prices (list): Список цен товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        dict: Результат операции.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Examples:
        >>> update_price([{"price": "5990", "offer_id": "123"}],
        >>>     "client123", "token123")

        >>> update_price("5990", "client123", "token123")
        # Вы можете увидеть ошибку BadRequest в терминале.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновить остатки товаров.

    Args:
        stocks (list): Список остатков товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        dict: Результат операции.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.
    Examples:
        >>> update_price([{"price": "5990", "offer_id": "123"}],
        >>>     "client123", "token123")
        {'result': 'success'}

        >>> update_price("5990", "client123", "token123")
        TypeError: expected list
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл остатков с сайта Casio.

    Returns:
        list: Список остатков часов.

    Raises:
        requests.exceptions.HTTPError: В случае возникновения HTTP-ошибки
        во время запроса.

    Examples:
        >>> download_stock()
        [{'item': 'watch123', 'quantity': 50},
        {'item': 'watch124', 'quantity': 30}]

        >>> download_stock("https://invalidurl.com")
        # Вы можете увидеть ошибку BadRequest в терминале.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создать список остатков товаров.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список остатков товаров.

    Examples:
        >>> create_stocks([{"Код": "123", "Количество": ">10"},
        >>>     {"Код": "456", "Количество": "1"}],["123", "789"])
        >>>     [{'offer_id': '123', 'stock': 100},
        >>>     {'offer_id': '456', 'stock': 0},
        {'offer_id': '789', 'stock': 0}]

        >>> create_stocks(
        >>>     [{"Код": "123", "Количество": ">10"},
        >>>     {"Код": "456", "Kоличество": "1"}], "invalid")
        TypeError: expected list

    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создать список цен товаров.

    Args:
        watch_remnants (list): Список остатков товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список цен товаров.

    Examples:
        >>> create_prices(
        >>>     [{"Код": "123", "Цена": "5'990.00 руб."},
        >>>     {"Код": "456", "Цена": "7'500.50 руб."}], ["123", "789"]
        >>> )
        [{
            'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
            'offer_id': '123', 'old_price': '0', 'price': '5990'},
        {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
         'offer_id': '456', 'old_price': '0', 'price': '7500'}]

        >>> create_prices(
        >>>     [{"Код": "123", "Цена": "5'990.00 руб."}, {"Код": "456",
        >>>       "Price": "7'500.50 руб."}], "invalid"
        >>> )
        # TypeError: expected list
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразовать цену. Пример: 5'990.00 руб. -> 5990

    Выполняет преобразование строки, представляющей цену товара, из одного
    формата в другой.

    Args:
        price (str): цена в формате строки
    Returns:
        str: значение в виде строки, представляющей цену товара без
        лишних символов
    Examples:
        Если на вход подается строка "5'990.00 руб.", функция вернет "5990",
        что представляет собой цену товара без знака "руб."
        и разделителя апострофа.
        >>> price_conversion("5'990.00 руб.")
        "5990"
        >>> price_conversion(5990.00)
        TypeError: expected string or bytes-like object
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделить список на части по заданному количеству элементов.

    Args:
        lst (list): Список элементов.
        n (int): Размер части списка.

    Yields:
        list: Список элементов, разделенный на части.

    Examples:
    >>> list(divide([1, 2, 3, 4, 5], 2)
    [[1, 2], [3, 4], [5]]

    >>> list(divide([1, 2, 3, 4, 5], "invalid")
    TypeError: expected list
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цены товаров на маркетплейс "Ozon".

    Args:
        watch_remnants (list[dict]): Список остатков часов
        с информацией о ценах.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        list: Список цен товаров с указанием валюты,
        загруженных на маркетплейс.

    Examples:
        >>> upload_prices(
        >>>     [{"Код": "123", "Цена": "5'990.00 руб."},
        >>>      {"Код": "456", "Цена": "7'500.50 руб."}],
        >>>     "client123", "token456"
        >>> )
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
          'offer_id': '123', 'old_price': '0', 'price': '5990'},
         {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
          'offer_id': '456', 'old_price': '0', 'price': '7500'}]

        >>> await upload_prices(
        >>>     [{"Код": "123", "Price": "5'990.00 руб."},
        >>>      {"Code": "456", "Price": "7'500.50 руб."}], 123, 456
        >>> )
        # Вы можете увидеть ошибку BadRequest в терминале.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загрузить остатки товаров на платформу Озон.

    Args:
        watch_remnants (list): Список остатков товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Returns:
        tuple: Две части - не пустые остатки и все остатки.

    Examples:
        >>> upload_stocks(
        >>>     [{"Код": "123", "Количество": "5"},
        >>>      {"Код": "456", "Количество": ">10"}],
        >>>      "client123",
        >>>      "token456"
        >>> )
        ([
            {'offer_id': '123', 'stock': 5},
            {'offer_id': '456', 'stock': 100}
        ], [
            {'offer_id': '123', 'stock': 5},
            {'offer_id': '456', 'stock': 100}
        ])

        >>> upload_stocks(
        >>>     [{"Code": "123", "Stock": "5"},
        >>>      {"Код": "456", "Stock": "10+"}],
        >>>     123,
        >>>     456
        >>> )
        # Вы можете увидеть ошибку BadRequest в терминале.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
