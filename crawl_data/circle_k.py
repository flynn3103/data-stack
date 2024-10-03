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

# 119493
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
        absolute_next_page_url = 'https://www.circlek.com.vn/vi/he-thong-circle-k/'
        print("url: {}".format(absolute_next_page_url))
        driver.get(absolute_next_page_url)
        time.sleep(1)

        driver.execute_script("window.scrollBy(0, 500);")
        scroll_container = driver.find_element(By.CLASS_NAME, 'mCustomScrollBox')
        store_element_lst = driver.find_elements(By.CSS_SELECTOR, '.detail-item .item')

       
        # Scroll incrementally and collect items
        store_items = []

        # Initialize a set to store seen store addresses
        seen_store = set()

        # Scroll and get data in a loop
        scroll_pause_time = 1  # Time to wait for loading new data
        scroll_amount = 90
        store_lst = []
        
        i = 0
        scrolling_flag = False
        while True:
            new_data = []
            store_element_lst_tmp = store_element_lst
            store_element_lst_tmp = store_element_lst_tmp[i:]
            scroll_extra = 0
            for store in store_element_lst_tmp:
                dummy = store
                if i % 1 == 0 and i != 0 and scrolling_flag == False:
                    scrolling_flag = True
                    break

                while True:
                    try:
                        if store.text == '':
                            driver.execute_script(f"arguments[0].scrollTop += 15;", scroll_container)
                            store = dummy
                        else:
                            break
                        # location_element = store.find_element(By.CSS_SELECTOR,'.click_location')
                        # if location_element is not None:
                        #     break
                    except:
                        driver.execute_script(f"arguments[0].scrollTop += 50;", scroll_container)

                location_element = store.find_element(By.CSS_SELECTOR,'.click_location')
                id = location_element.get_attribute('data-index')

                try:
                    pic_element = store.find_element(By.CLASS_NAME,'clearfix')
                    pic_sub_element = pic_element.find_element(By.CLASS_NAME,'list-ser-24h')
                    if pic_sub_element is not None:
                        print('--')
                        scroll_extra += 80
                except:
                    scroll_extra += 0
                
                if id not in seen_store:
                    seen_store.add(id)

                    address = store.text.split('\n')
                    province = address[-2]
                    address = ', '.join(address[:-1])
                    print(address)
                    # get location
                    
                    lat = location_element.get_attribute('data-lat')
                    long = location_element.get_attribute('data-lng')
                    print(f'{lat,long}')
                    store_lst.append([province,address,lat,long])
                    time.sleep(0.01)

                   
                    scrolling_flag = False
                i += 1

            # driver.execute_script("arguments[0].scrollTop += arguments[1];",scrollable_div, 100)
            driver.execute_script(f"arguments[0].scrollTop += {scroll_amount + scroll_extra};", scroll_container)
           
            if store_element_lst_tmp == []:
                break
           
        
        driver.quit()
    
        #load to excel workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        col_name = ['Tỉnh/Thành phố','Địa chỉ','Latitude','Longitude']
        ws.append(col_name)

        for store in store_lst:
            ws.append(store)
        wb.save(filename='circlek.xlsx')

    except Exception as ex:
        print(ex)
        driver.quit()
# 65481