import boto3
import logging
import json
import pandas as pd 
from io import BytesIO
import math
logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)

'''Utility and helper functions which are called by the transform data lambda'''





def get_bucket_names():
    '''Obtains the full names of the processed and ingested buckets in s3 regardless of the randomised suffix'''
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        if "ingested" in bucket.name:
            ingested_bucket = bucket.name
        if "processed" in bucket.name:
            processed_bucket = bucket.name
    return [ingested_bucket, processed_bucket]


def get_file_names(bucket_name, prefix):
    '''Obtains the names of all the files stored in the relevant s3 bucket'''
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=bucket_name,Prefix=prefix)
    try:
        return [file['Key'] for file in response['Contents']]
    except KeyError:
        logger.error("ERROR! No files found")
        return []


def get_file_contents(bucket_name, file_name):
    '''Obtains the contents of the given file in the relevant bucket'''
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    jsonfile = json.loads(response['Body'].read())
    return jsonfile


def write_file_to_processed_bucket(bucket_name, key, list):
    '''Converts the formatted data into a parquet file, and then writes the file to a given s3 bucket'''
    s3 = boto3.client('s3')
    pandadataframe = pd.DataFrame(list)
    out_buffer = BytesIO()
    pandadataframe.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=key, Body=out_buffer.getvalue())

'''The following functions format ingested data from the totesys database, into the    
pattern needed for the relevant table in the new star schema. Each function is 
named for the table they will load into'''

def format_counterparty(raw_counter, raw_address):

    formattedList = []
    for counter in raw_counter:
        new_details = {}
        try:
            match = find_match(
                "legal_address_id",
                "address_id",
                counter,
                raw_address)
            prefix = "counterparty_legal_"
            new_details["counterparty_id"] = counter["counterparty_id"]
            new_details[f"{prefix}name"] = counter[f"{prefix}name"]
            new_details[f"{prefix}address_line_1"] = match["address_line_1"]
            new_details[f"{prefix}address_line_2"] = match["address_line_2"]
            new_details[f"{prefix}district"] = match["district"]
            new_details[f"{prefix}city"] = match["city"]
            new_details[f"{prefix}postal_code"] = match["postal_code"]
            new_details[f"{prefix}country"] = match["country"]
            new_details[f"{prefix}phone_number"] = match["phone"]
            formattedList.append(new_details)
        except IndexError:
            logger.error(f'{counter} no matching table found')

            continue

    return formattedList


def format_currency(raw_currency):
    currency_names = {

        "AED": "United Arab Emirates Dirham",
        "AFN": "Afghan Afghani",
        "ALL": "Albanian Lek",
        "AMD": "Armenian Dram",
        "ANG": "Netherlands Antillean Guilder",
        "AOA": "Angolan Kwanza",
        "ARS": "Argentine Peso",
        "AUD": "Australian Dollar",
        "AWG": "Aruban Florin",
        "AZN": "Azerbaijani Manat",
        "BAM": "Bosnia-Herzegovina Convertible Mark",
        "BBD": "Barbadian Dollar",
        "BDT": "Bangladeshi Taka",
        "BGN": "Bulgarian Lev",
        "BHD": "Bahraini Dinar",
        "BIF": "Burundian Franc",
        "BMD": "Bermudan Dollar",
        "BND": "Brunei Dollar",
        "BOB": "Bolivian Boliviano",
        "BRL": "Brazilian Real",
        "BSD": "Bahamian Dollar",
        "BTC": "Bitcoin",
        "BTN": "Bhutanese Ngultrum",
        "BWP": "Botswanan Pula",
        "BYR": "Belarusian Ruble",
        "BZD": "Belize Dollar",
        "CAD": "Canadian Dollar",
        "CDF": "Congolese Franc",
        "CHF": "Swiss Franc",
        "CLF": "Chilean Unit of Account (UF)",
        "CLP": "Chilean Peso",
        "CNY": "Chinese Yuan",
        "COP": "Colombian Peso",
        "CRC": "Costa Rican Col\u00f3n",
        "CUC": "Cuban Convertible Peso",
        "CUP": "Cuban Peso",
        "CVE": "Cape Verdean Escudo",
        "CZK": "Czech Republic Koruna",
        "DJF": "Djiboutian Franc",
        "DKK": "Danish Krone",
        "DOP": "Dominican Peso",
        "DZD": "Algerian Dinar",
        "EEK": "Estonian Kroon",
        "EGP": "Egyptian Pound",
        "ERN": "Eritrean Nakfa",
        "ETB": "Ethiopian Birr",
        "EUR": "Euro",
        "FJD": "Fijian Dollar",
        "FKP": "Falkland Islands Pound",
        "GBP": "British Pound Sterling",
        "GEL": "Georgian Lari",
        "GGP": "Guernsey Pound",
        "GHS": "Ghanaian Cedi",
        "GIP": "Gibraltar Pound",
        "GMD": "Gambian Dalasi",
        "GNF": "Guinean Franc",
        "GTQ": "Guatemalan Quetzal",
        "GYD": "Guyanaese Dollar",
        "HKD": "Hong Kong Dollar",
        "HNL": "Honduran Lempira",
        "HRK": "Croatian Kuna",
        "HTG": "Haitian Gourde",
        "HUF": "Hungarian Forint",
        "IDR": "Indonesian Rupiah",
        "ILS": "Israeli New Sheqel",
        "IMP": "Manx pound",
        "INR": "Indian Rupee",
        "IQD": "Iraqi Dinar",
        "IRR": "Iranian Rial",
        "ISK": "Icelandic Kr\u00f3na",
        "JEP": "Jersey Pound",
        "JMD": "Jamaican Dollar",
        "JOD": "Jordanian Dinar",
        "JPY": "Japanese Yen",
        "KES": "Kenyan Shilling",
        "KGS": "Kyrgystani Som",
        "KHR": "Cambodian Riel",
        "KMF": "Comorian Franc",
        "KPW": "North Korean Won",
        "KRW": "South Korean Won",
        "KWD": "Kuwaiti Dinar",
        "KYD": "Cayman Islands Dollar",
        "KZT": "Kazakhstani Tenge",
        "LAK": "Laotian Kip",
        "LBP": "Lebanese Pound",
        "LKR": "Sri Lankan Rupee",
        "LRD": "Liberian Dollar",
        "LSL": "Lesotho Loti",
        "LTL": "Lithuanian Litas",
        "LVL": "Latvian Lats",
        "LYD": "Libyan Dinar",
        "MAD": "Moroccan Dirham",
        "MDL": "Moldovan Leu",
        "MGA": "Malagasy Ariary",
        "MKD": "Macedonian Denar",
        "MMK": "Myanma Kyat",
        "MNT": "Mongolian Tugrik",
        "MOP": "Macanese Pataca",
        "MRO": "Mauritanian Ouguiya",
        "MUR": "Mauritian Rupee",
        "MVR": "Maldivian Rufiyaa",
        "MWK": "Malawian Kwacha",
        "MXN": "Mexican Peso",
        "MYR": "Malaysian Ringgit",
        "MZN": "Mozambican Metical",
        "NAD": "Namibian Dollar",
        "NGN": "Nigerian Naira",
        "NIO": "Nicaraguan C\u00f3rdoba",
        "NOK": "Norwegian Krone",
        "NPR": "Nepalese Rupee",
        "NZD": "New Zealand Dollar",
        "OMR": "Omani Rial",
        "PAB": "Panamanian Balboa",
        "PEN": "Peruvian Nuevo Sol",
        "PGK": "Papua New Guinean Kina",
        "PHP": "Philippine Peso",
        "PKR": "Pakistani Rupee",
        "PLN": "Polish Zloty",
        "PYG": "Paraguayan Guarani",
        "QAR": "Qatari Rial",
        "RON": "Romanian Leu",
        "RSD": "Serbian Dinar",
        "RUB": "Russian Ruble",
        "RWF": "Rwandan Franc",
        "SAR": "Saudi Riyal",
        "SBD": "Solomon Islands Dollar",
        "SCR": "Seychellois Rupee",
        "SDG": "Sudanese Pound",
        "SEK": "Swedish Krona",
        "SGD": "Singapore Dollar",
        "SHP": "Saint Helena Pound",
        "SLL": "Sierra Leonean Leone",
        "SOS": "Somali Shilling",
        "SRD": "Surinamese Dollar",
        "STD": "S\u00e3o Tom\u00e9 and Pr\u00edncipe Dobra",
        "SVC": "Salvadoran Col\u00f3n",
        "SYP": "Syrian Pound",
        "SZL": "Swazi Lilangeni",
        "THB": "Thai Baht",
        "TJS": "Tajikistani Somoni",
        "TMT": "Turkmenistani Manat",
        "TND": "Tunisian Dinar",
        "TOP": "Tongan Pa\u02bbanga",
        "TRY": "Turkish Lira",
        "TTD": "Trinidad and Tobago Dollar",
        "TWD": "New Taiwan Dollar",
        "TZS": "Tanzanian Shilling",
        "UAH": "Ukrainian Hryvnia",
        "UGX": "Ugandan Shilling",
        "USD": "United States Dollar",
        "UYU": "Uruguayan Peso",
        "UZS": "Uzbekistan Som",
        "VEF": "Venezuelan Bol\u00edvar Fuerte",
        "VND": "Vietnamese Dong",
        "VUV": "Vanuatu Vatu",
        "WST": "Samoan Tala",
        "XAF": "CFA Franc BEAC",
        "XAG": "Silver (troy ounce)",
        "XAU": "Gold (troy ounce)",
        "XCD": "East Caribbean Dollar",
        "XDR": "Special Drawing Rights",
        "XOF": "CFA Franc BCEAO",
        "XPF": "CFP Franc",
        "YER": "Yemeni Rial",
        "ZAR": "South African Rand",
        "ZMK": "Zambian Kwacha (pre-2013)",
        "ZMW": "Zambian Kwacha",
        "ZWL": "Zimbabwean Dollar"

    }
    formatted_currency = []
    for currency_data in raw_currency:
        new_details = {}
        new_details['currency_id'] = currency_data['currency_id']
        new_details['currency_code'] = currency_data['currency_code']
        try:
            new_details['currency_name'] = currency_names[currency_data['currency_code']]
        except KeyError:
            new_details['currency_name'] = "Unrecognised"
            logger.error(
                f'{currency_data["currency_code"]} no matching currency found')
            continue
        formatted_currency.append(new_details)
    return formatted_currency


def format_design(raw_designs):

    formatted = remove_keys(raw_designs)
    return (formatted)


def format_location(raw_locations):

    formatted = remove_keys(raw_locations)
    for dictionary in formatted:
        dictionary["location_id"] = dictionary["address_id"]
        dictionary.pop("address_id")
    return (formatted)


def format_payment_type(raw_data):
    formatted_data = remove_keys(raw_data)
    return formatted_data

def format_transaction(raw_data):
    formatted_data=remove_keys(raw_data)
    return formatted_data


def format_payments(raw_data):
    time_formatted_data = [time_splitter(payment) for payment in raw_data]
    formatted_data = remove_keys(time_formatted_data, remove=[
                                 'company_ac_number', 'counterparty_ac_number'])
    return formatted_data


def format_purchase(raw_purchase_data):
    formatted_purchases = [time_splitter(purchase)
                           for purchase in raw_purchase_data]

    return formatted_purchases


def format_sales_facts(raw_sales_data):
    formatted_sales = []

    for sale in raw_sales_data:

        new_details = {}
        for key in sale:

            if key == "staff_id":
                new_details["sales_staff_id"] = sale[key]
            elif key == "created_at":
                new_details['created_date'] = sale['created_at'].split(' ')[0]
                new_details['created_time'] = sale['created_at'].split(' ')[1]
            elif key == "last_updated":
                new_details['last_updated_date'] = sale['last_updated'].split(' ')[
                    0]
                new_details['last_updated_time'] = sale['last_updated'].split(' ')[
                    1]
            else:
                new_details[key] = sale[key]

        formatted_sales.append(new_details)

    return formatted_sales


def format_staff(unformatted_staff, unformatted_depts):
    formatted_staff = []
    for staff in unformatted_staff:
        new_details = {}
        try:
            new_details['staff_id'] = staff['staff_id']
            new_details['first_name'] = staff['first_name']
            new_details['last_name'] = staff['last_name']
            match = find_match(
                "department_id",
                "department_id",
                staff,
                unformatted_depts)
            new_details['department_name'] = match['department_name']
            new_details['location'] = match['location']
            new_details['email_address'] = staff['email_address']
            formatted_staff.append(new_details)
        except IndexError:
            logger.error(f'{staff} no matching table found')

            continue

    return formatted_staff   


def find_match(primarykey, secondarykey, target, search_list):
    '''A helper function for the formatting functions, this function matches the target dictionary to 
    the dictionary with the primary secondary match in the list given'''
    result = [match for match in search_list if match[secondarykey]
              == target[primarykey]][0]
    return result


def remove_keys(list,remove=["created_at","last_updated"]):
    '''Will remove the given keys (default created at and last updated) from a list of dictionaries, while not 
    mutating'''
    return [{key:dictionary[key] for key in dictionary if key not in remove} for dictionary in list]


def time_splitter(dictionary):
        '''Splits the created and last updated keys into a time key and a date key, for use
        in the star schema as dim and fact.'''
    
        new_details = {}
        for key in dictionary:
            if key == "created_at":
                new_details['created_date'] = dictionary['created_at'].split(' ')[0]
                new_details['created_time'] = dictionary['created_at'].split(' ')[1]
            elif key == "last_updated":
                new_details['last_updated_date'] = dictionary['last_updated'].split(' ')[0]
                new_details['last_updated_time'] = dictionary['last_updated'].split(' ')[1]
            else:
                new_details[key]=dictionary[key]
        return new_details
        
   
