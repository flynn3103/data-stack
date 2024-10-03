import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import xml.etree.ElementTree as ET
import ssl
import urllib3
import pandas as pd

import re

# Create a custom SSL context
ssl_context = ssl.create_default_context()
ssl_context.options |= 0x4  # This enables the "unsafe legacy renegotiation" option.

# Create a custom adapter to use the SSL context
class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = ssl_context
        return super().init_poolmanager(*args, **kwargs)
    
    def proxy_manager_for(self, *args, **kwargs):
        kwargs['ssl_context'] = ssl_context
        return super().proxy_manager_for(*args, **kwargs)

# Function to get XML content with retry and timeout
def get_xml_with_retries(url, timeout=10, retries=3):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = SSLAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# URL to the XML file
url = 'https://scsanctions.un.org/resources/xml/en/consolidated.xml'

# Get XML content with retries and timeout
xml_content = get_xml_with_retries(url, timeout=10, retries=5)

# Check if the request was successful
if xml_content:
    # Parse XML content
    root = ET.fromstring(xml_content)
    
    # Extract all individuals
    individuals = root.findall('.//INDIVIDUAL')
    
    # Function to extract text from an element
    def get_text(element, tag):
        return element.find(tag).text if element.find(tag) is not None else None
    
    # List to store all individual data
    individuals_data_df = pd.DataFrame()
    
    for individual in individuals:
        data = {
            'DATAID': get_text(individual, 'DATAID'),
            'VERSIONNUM': get_text(individual, 'VERSIONNUM'),
            'FIRST_NAME': get_text(individual, 'FIRST_NAME'),
            'SECOND_NAME': get_text(individual, 'SECOND_NAME'),
            'THIRD_NAME': get_text(individual, 'THIRD_NAME'),
            'UN_LIST_TYPE': get_text(individual, 'UN_LIST_TYPE'),
            'REFERENCE_NUMBER': get_text(individual, 'REFERENCE_NUMBER'),
            'LISTED_ON': get_text(individual, 'LISTED_ON'),
            'NAME_ORIGINAL_SCRIPT': get_text(individual, 'NAME_ORIGINAL_SCRIPT'),
            # 'COMMENTS1': get_text(individual, 'COMMENTS1'),
            'TITLES': '; '.join([title.text for title in individual.findall('.//TITLE/VALUE')]),
            'DESIGNATIONS': '; '.join([designation.text for designation in individual.findall('.//DESIGNATION/VALUE')]),
            'NATIONALITIES': '; '.join([nationality.text for nationality in individual.findall('.//NATIONALITY/VALUE')]),
            'LIST_TYPES': '; '.join([list_type.text for list_type in individual.findall('.//LIST_TYPE/VALUE')]),
            'LAST_DAYS_UPDATED': [last_day.text for last_day in individual.findall('.//LAST_DAY_UPDATED/VALUE')],
            'TYPE': 'Individual',
        }


        if data['LAST_DAYS_UPDATED']:
            if len(data['LAST_DAYS_UPDATED']) == 1:
                data['LAST_DAYS_UPDATED'] = data['LAST_DAYS_UPDATED'][0]
            else:
                data['LAST_DAYS_UPDATED'] = max(data['LAST_DAYS_UPDATED'])
        if not data['LAST_DAYS_UPDATED']:
            data['LAST_DAYS_UPDATED'] = None
        
        individuals_data_df = pd.concat([individuals_data_df, pd.DataFrame([data], index=[0])], ignore_index=True)
       
    
    # Print all individual data
    # for data in individuals_data_df:
    #     print(data)


    # Extract all entities
    entities = root.findall('.//ENTITY')
    
    # List to store all entity data
    entities_data_df = pd.DataFrame()
    
    for entity in entities:
        data = {
            'DATAID': get_text(entity, 'DATAID'),
            'VERSIONNUM': get_text(entity, 'VERSIONNUM'),
            'FIRST_NAME': get_text(entity, 'FIRST_NAME'),
            'UN_LIST_TYPE': get_text(entity, 'UN_LIST_TYPE'),
            'REFERENCE_NUMBER': get_text(entity, 'REFERENCE_NUMBER'),
            'LISTED_ON': get_text(entity, 'LISTED_ON'),
            # 'COMMENTS1': get_text(entity, 'COMMENTS1'),
            'LIST_TYPES': '; '.join([list_type.text for list_type in entity.findall('.//LIST_TYPE/VALUE')]),
            'LAST_DAYS_UPDATED': [last_day.text for last_day in entity.findall('.//LAST_DAY_UPDATED/VALUE')],
            'ALIAS_NAME': '; '.join(filter(None, [
                # {
                #     'QUALITY': alias.find('QUALITY').text if alias.find('QUALITY') is not None else None,
                #     'ALIAS_NAME': alias.find('ALIAS_NAME').text if alias.find('ALIAS_NAME') is not None else None
                # }
                # for alias in entity.findall('.//ENTITY_ALIAS')
               
                alias.find('ALIAS_NAME').text if alias.find('ALIAS_NAME') is not None else None
                for alias in entity.findall('.//ENTITY_ALIAS')
            ])),
            'ADDRESS': '; '.join([', '.join(filter(None, [
                addr.find('STREET').text if addr.find('STREET') is not None else None,
                addr.find('CITY').text  if addr.find('CITY') is not None else None,
                addr.find('COUNTRY').text if addr.find('COUNTRY') is not None else None,
            ]))
                for addr in entity.findall('.//ENTITY_ADDRESS')
            ]),
            'TYPE': 'Entity',
        }

        # hanlde first name
        if len(data['FIRST_NAME']) > 100:
            cleaned_text = re.sub(r"\s+", " ", data['FIRST_NAME'])
            data['FIRST_NAME'] = cleaned_text

        # handle alias name
        if len(data['ALIAS_NAME']) > 200:
            cleaned_text = re.sub(r"\s+", " ", data['ALIAS_NAME'])
            data['ALIAS_NAME'] = cleaned_text

    
        if data['LAST_DAYS_UPDATED']:
            if len(data['LAST_DAYS_UPDATED']) == 1:
                data['LAST_DAYS_UPDATED'] = data['LAST_DAYS_UPDATED'][0]
            else:
                data['LAST_DAYS_UPDATED'] = max(data['LAST_DAYS_UPDATED'])
        if not data['LAST_DAYS_UPDATED']:
            data['LAST_DAYS_UPDATED'] = None

        

        entities_data_df = pd.concat([entities_data_df, pd.DataFrame([data], index=[0])], ignore_index=True)

    # # Print all individual data
    # for data in entities_data:
    #     print(data)
      
else:
    print("Failed to retrieve XML content after retries.")



df = pd.concat([individuals_data_df, entities_data_df], axis=0, ignore_index=True, sort=False)

df.to_csv("SANCTION.csv")
# save to DB