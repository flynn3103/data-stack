# import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import openpyxl
import re
import time 

from selenium.webdriver.common.by import By 

#TODO: Web Source Changed Frequently, Web Lord ????

if __name__=="__main__":
    timeout = 20

    options = Options()
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')  # Set window size for headless mode
    driver = webdriver.Chrome(options=options)

    last_height = 0
    try:
        store_lst = []
        absolute_next_page_url = 'https://www.famima.vn/danh-sach-cua-hang/'
        print("url: {}".format(absolute_next_page_url))
        driver.get(absolute_next_page_url)
        time.sleep(3)

        #handle pop-up
        popup_element = driver.find_element(By.CLASS_NAME,'mfp-content')
        popup_btn_close = popup_element.find_element(By.CLASS_NAME,'mfp-close')
        popup_btn_close.click()

        #scroll down to middle window
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(2)

        grid_containner = driver.find_element(By.CLASS_NAME,'dvls_maps_sidebar')
        store_element_lst = grid_containner.find_elements(By.CLASS_NAME,'dvls_result_item')

        for store_element in store_element_lst:
            store = store_element.find_element(By.CLASS_NAME,'dvls_result_infor')
            store_info = store.text.split('\n')
            store_name = store_info[0]
            store_address = store_info[1]
            print(store_address)

            location_element = store.find_element(By.TAG_NAME,'a')
            location = location_element.get_attribute('href')
            lat, long = location.split('=')[-1].split(',')
            print(f'{lat,long}')

            store_lst.append([store_name,store_address,lat,long])
            time.sleep(0.1)
           
        
        driver.quit()
    
        #load to excel workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        col_name = ['Tên','Địa chỉ','Latitude','Longitude']
        ws.append(col_name)

        for store in store_lst:
            ws.append(store)
        wb.save(filename='Family Mart.xlsx')

        print('FINISH')

    except Exception as ex:
        print(ex)
        driver.quit()
# 65481