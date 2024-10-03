# import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import openpyxl
import re
import time 

# from xlwt import Workbook
from selenium.webdriver.common.by import By 


if __name__=="__main__":

    #driver = uc.Chrome(version_main=114)
    # service = Service(executable_path="D:\TGT\Crawl_data\Driver\chromedriver.exe")
    # chrome_options = Options()
    # chrome_options.add_argument("--start-maximized")
    # driver = webdriver.Chrome(service=service,options=chrome_options)

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)

    
    try:
        
        absolute_next_page_url = 'https://www.thegioididong.com/he-thong-sieu-thi-the-gioi-di-dong'
        print("url: {}".format(absolute_next_page_url))
        driver.get(absolute_next_page_url)
        time.sleep(2)

        #get 63 url of provinces
        url_region_lst = []
        store_lst = []
        for i in range(1,64):
            button = driver.find_elements(By.XPATH,f'//*[@id="province-box__store"]/div/div/ul/li[{i}]/a')
            sub_href = button[0].get_attribute('href')
            region = button[0].get_attribute('text').strip()
            url_region_lst.append((region,sub_href))
        print('Collect successfully: ',len(url_region_lst))

        #get data of each province
        for province in url_region_lst:
            driver.get(province[1])
            time.sleep(2)
            num_stores = driver.find_element(By.XPATH,'//*[@id="homeTGDD"]/div[2]/aside/div[3]/div').text
            num_stores = int(re.findall(r'\d+', num_stores)[0])
            print(province[0],': ',num_stores, 'cửa hàng')

            #get data of each store
            for i in range(1,num_stores+1):
                store_address = driver.find_element(By.XPATH,f'//*[@id="homeTGDD"]/div[2]/aside/div[3]/ul/li[{i}]/a[1]').text
                store_map_url = driver.find_element(By.XPATH,f'//*[@id="homeTGDD"]/div[2]/aside/div[3]/ul/li[{i}]/a[2]').get_attribute('href')
                store_lat, store_long = store_map_url.split('=')[-1].split(',')

                #handle 'see more' button
                if i%10 == 0 and i!=num_stores and num_stores>10:
                    button = driver.find_element(By.XPATH,'//*[@id="homeTGDD"]/div[2]/aside/div[3]/a')
                    button.click()
                    time.sleep(1)
                store_lst.append([province[0],
                                  store_address,
                                  store_map_url,
                                  store_lat,
                                  store_long])
                print(f'{i}. {store_address}')
            print('\n')
                
        driver.quit()
    
        #load to excel workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        col_name = ['Tỉnh\Thành phố','Địa chỉ','URL google map','Latitude','Longitude']
        ws.append(col_name)

        for store in store_lst:
            ws.append(store)
        wb.save(filename='TGDD.xlsx')

    except Exception as ex:
        print(ex)
        driver.quit()
        pass