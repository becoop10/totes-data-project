from star_schema.src.utils.helpers import find_match, remove_keys, time_splitter
import logging
import datetime
import pandas as pd
import math

logger = logging.getLogger('DBTransformationLogger')
logger.setLevel(logging.INFO)


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

def format_date():
    formatted_datetime = []
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    for d in pd.date_range(start='11/03/2022', end=datetime.datetime.now()):
        new_date = {}

        new_date['date_id'] = d
        new_date['year'] = d.year
        new_date['month'] = d.month #1-12
        new_date['day'] = d.day
        new_date['day_of_week'] = d.isoweekday() #1-7, mon-sun
        new_date['day_name'] = days[d.weekday()]
        new_date['month_name'] = months[d.month - 1]
        new_date['quarter'] = math.ceil(d.month / 3 )

        formatted_datetime.append(new_date)
     
    return formatted_datetime