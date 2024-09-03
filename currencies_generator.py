import datetime
import json

from forex_python.converter import CurrencyRates
import boto3

s3 = boto3.client('s3')

currency_rates = CurrencyRates()

# list of currencies you want to convert from
from_currency_list = ['USD', 'EUR', 'GBP']
target_currency = 'IND'
# in this bucket results will be stored
DESTINATION_BUCKET = 'currency-data-bucket-cdk-project'


def get_currencies(
        from_currency_list:list[str],
        target_currency:str,
        date_obj:datetime.date=datetime.date.today()
        ) -> dict:

    currency_data:dict = {
        'target_currency': target_currency
        }

    for currency in from_currency_list:
        exchange_rate = currency_rates.get_rate(currency, target_currency, date_obj=date_obj)
        currency_data[currency] = exchange_rate
    return currency_data


def generate_key_prefix_from_date(date:datetime.date):
    return f'year={date.year}/month={date.month}/day={date.day}'


def generate_bucket_key_for_obj(key_prefix:str, obj_name:str) -> str:
    return f'{key_prefix}/{obj_name}'


def generate_currencies_for_number_of_days_and_store_in_bucket(number_of_days:int, bucket_name:str) -> None:
    for i in range(number_of_days):
        past_date:datetime.date = datetime.date.today() - datetime.timedelta(days=i)
        currency_data_for_date:dict = get_currencies(
            from_currency_list=from_currency_list,
            target_currency=target_currency,
            date_obj=past_date)
        currency_data_as_json = json.dumps(currency_data_for_date)
        object_prefix = generate_key_prefix_from_date(past_date)
        object_key = generate_bucket_key_for_obj(object_prefix, f'currency_data={past_date}.json')
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=currency_data_as_json)
        print(f'obj with key {object_key} stored successfully in {bucket_name}')


if __name__ == '__main__':
    generate_currencies_for_number_of_days_and_store_in_bucket(
        30,
        bucket_name=DESTINATION_BUCKET
        )