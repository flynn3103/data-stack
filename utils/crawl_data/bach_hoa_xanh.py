from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import openpyxl
import time

# TODO: Can not crawl Bach Hoa Xanh
def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    driver = webdriver.Chrome(options=options)
    return driver

def load_provinces(driver, timeout):
    driver.find_element(By.XPATH, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[1]/div').click()
    province_elements = driver.find_element(By.XPATH, '//*[@id="dropdown-open"]/div/div[2]/div[2]').text
    return province_elements.split('\n')

def select_dropdown(driver, timeout, xpath):
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath))).click()

def extract_store_data(driver, province, address, latitude, longitude):
    return [province, address, latitude, longitude]

def save_to_excel(result, filename='BachHoaXanh.xlsx'):
    wb = openpyxl.Workbook()
    ws = wb.active
    col_name = ['Tỉnh/Thành phố', 'Địa chỉ', 'Latitude', 'Longitude']
    ws.append(col_name)

    for store in result:
        ws.append(store)
    wb.save(filename=filename)
    print('Data saved to Excel.')

if __name__ == "__main__":
    timeout = 20
    driver = setup_driver()
    result = []

    absolute_next_page_url = 'https://www.bachhoaxanh.com/he-thong-sieu-thi'
    driver.get(absolute_next_page_url)
    time.sleep(1)

    province_lst = load_provinces(driver, timeout)
    
    for i in range(1, len(province_lst) + 1):
        select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[1]/div')
        select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{i}]')

        district_elements = driver.find_element(By.XPATH, '//*[@id="dropdown-open"]/div/div[2]/div[2]').text
        district_lst = district_elements.split('\n')
        time.sleep(3)

        for j in range(1, len(district_lst) + 1):
            select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[2]/div')
            select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{j}]')
            time.sleep(3)

            ward_elements = driver.find_element(By.XPATH, '//*[@id="dropdown-open"]/div/div[2]/div[2]').text
            ward_lst = ward_elements.split('\n')
            time.sleep(3)

            for k in range(1, len(ward_lst)):
                select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[3]/div')
                select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{k}]')
                time.sleep(3)

                store_elements = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'text-basic-800'))
                )

                while True:
                    time.sleep(1)
                    try:
                        btn_more = store_elements.find_element(By.CLASS_NAME, 'text-center')
                        if btn_more:
                            btn_more.click()
                    except:
                        break

                store_lst = store_elements.find_elements(By.XPATH, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[5]/ul/li')

                for s in range(1, len(store_lst) + 1):
                    original_window = driver.current_window_handle
                    store = store_elements.find_element(By.XPATH, f'//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[5]/ul/li[{s}]')

                    address = store.find_element(By.CLASS_NAME, 'font-bold').text
                    print(address)

                    try:
                        direction_btn = store.find_element(By.CLASS_NAME, 'text-primary-500')
                        if direction_btn:
                            direction_btn.click()

                            WebDriverWait(driver, timeout).until(EC.new_window_is_opened)
                            windows = driver.window_handles

                            for window in windows:
                                if window != original_window:
                                    driver.switch_to.window(window)
                                    break
                            time.sleep(0.2)
                            new_url = driver.current_url
                            location = new_url.split('/')[-1]
                            latitude, longitude = location.split(',')
                            print(f'{latitude}, {longitude}')

                            result.append(extract_store_data(province_lst[i - 1], address, latitude, longitude))
                            driver.close()
                            time.sleep(2)
                            driver.switch_to.window(original_window)
                            time.sleep(2)
                            driver.back()
                            time.sleep(3)

                            driver.execute_script("window.scrollTo(0, 0);")
                            time.sleep(3)

                            select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[1]/div')
                            time.sleep(3)
                            select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{i}]')
                            time.sleep(3)
                            select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[2]/div')
                            time.sleep(3)
                            select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{j}]')
                            time.sleep(3)
                            select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[3]/div')
                            time.sleep(3)
                            select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{k}]')
                            time.sleep(1)
                    except ValueError as e:
                        print('Empty')
                        print(e)
                        select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[1]/div')
                        time.sleep(3)
                        select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{i}]')
                        time.sleep(3)
                        select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[2]/div')
                        time.sleep(3)
                        select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{j}]')
                        time.sleep(3)
                        select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[3]/div')
                        time.sleep(3)
                        select_dropdown(driver, timeout, f'//*[@id="dropdown-open"]/div/div[2]/div[2]/div[{k}]')
                        time.sleep(3)
                        continue

                select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[3]/div')
                time.sleep(3)

            select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[2]/div')
            time.sleep(3)

        select_dropdown(driver, timeout, '//*[@id="main-layout min-height-layout"]/div/div[1]/div[1]/div[4]/div[1]/div')
        time.sleep(3)

    save_to_excel(result)
    print('FINISH')
    driver.quit()