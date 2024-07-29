import os
import re
import datetime
import glob
import json
import hashlib
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dataset_utils import trim_filename,replace_tilde_chars,clean_filename,find_date

# create the "jurisprudencia-corte-suprema" directory if it doesn't exist
if not os.path.exists("dataset/jurisprudencia-corte-suprema"):
    os.mkdir("dataset/jurisprudencia-corte-suprema")

# set up the Chrome driver
driver = webdriver.Chrome()
# set filter by year because the page cant show entries beyond 100.000 results
year=int(datetime.datetime.now().year)
# navigate to the website
driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p=25&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n%5B5%2C1%5D%7CTribunal/CORTE%20SUPREMA%20DE%20JUSTICIA%20DE%20LA%20NACION%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")

existent=os.listdir("./dataset/jurisprudencia-corte-suprema")
while True:
    print(driver.current_url)
    # wait for the page to load
    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "result-item")))

    # extract the result-items
    result_items = driver.find_elements(By.CSS_SELECTOR, ".result-item:not([id='vista-documento'])")


    # iterate over the result-items and extract the name and link
    for item in result_items:
        link_element = item.find_element(By.TAG_NAME, "a")
        link = link_element.get_attribute("href")
        URLhash = str(hashlib.sha1(link.split("123456789")[0].encode('utf-8')).hexdigest())
        filename = os.path.join("dataset/jurisprudencia-corte-suprema", URLhash)
        existent=os.listdir("./dataset/jurisprudencia-corte-suprema")

        file_exists=False
        for file in existent:
            if URLhash in file:
                print(f"{URLhash} ya existe")
                #os.rename(nombre viejo, URLhash)
                file_exists=True
                break
        
        if not file_exists:
            # navigate to the link
            driver.get(link)
            # wait for the content to load
            wait.until(EC.presence_of_element_located((By.ID, "texto")))

            # extract the content
            content_element = driver.find_element(By.ID, "texto")
            content = content_element.get_attribute("innerHTML")

            soup = BeautifulSoup(content, 'html.parser')
            
            # Find linked content
            pdf_link = soup.find('a', {'class': 'externo', 'href': lambda href: href.endswith('.pdf')})
            fallos_a_los_que_aplica= soup.find('div', {'class': 'seccion'})
            soup.find('div',class_='reset').decompose()
            soup.find('div',class_='tab-panel').decompose()
            con_rel = soup.find('div',class_='contenido-relacionado')
            if con_rel:
                soup.find('div',class_='contenido-relacionado').decompose()
                con_rel=None
            otros_sumarios = soup.find('div',class_='otros-sumarios')
            if otros_sumarios:
                soup.find('div',class_='otros-sumarios').decompose()
                con_rel=None

            # remove HTML tags from the content and cleanup
            content = soup.get_text()
            content = '\n'.join([i for i in content.split('\n') if i.strip()])
            content = content.replace('SUMARIO DE FALLO', '\nSUMARIO DE FALLO ')
            content = content.replace('Magistrados', '\nMagistrados')
            content = content.replace('Id SAIJ', '\Id SAIJ')
            content = content.replace('SUMARIO', '\nSUMARIO ')
            content = content.replace('SINTESIS', '\nSINTESIS ')
            content = content.replace('TEXTO', '\nTEXTO ')
            content = content.replace('SENTENCIA', '\nSENTENCIA ')
            content = content.replace('FALLOS A LOS QUE APLICA', '\nFALLOS A LOS QUE APLICA ')
            content = content.replace('Ver archivo adjunto', '')
            JSONcontent = {"url": link, "content": content, "date": find_date(content), "scrapped_date":str(datetime.datetime.now()), "attached_file":None}
            #filename = filename.split('\\')[0]+"\\"+find_date(content)+"_"+filename.split('\\')[1]

            if fallos_a_los_que_aplica:
                fallos_a_los_que_aplica_link= fallos_a_los_que_aplica.find('a')
                if fallos_a_los_que_aplica_link:
                    content+= '\nLink a fallo al que aplica: ' + fallos_a_los_que_aplica_link.get('href')
            if pdf_link:
                link_str = pdf_link.get('href')
                JSONcontent['attached_file']=link_str
                content+= '\nContenido adjunto: http://www.saij.gob.ar' + link_str

            # save the content to a text file
            with open(filename+".json", "w", encoding="utf-8") as f:
                f.write(json.dumps(JSONcontent))
            driver.back()

    # check if there is a "Next" button
    next_button = driver.find_element(By.ID, "paginador-boton-siguiente")
    if next_button.is_displayed():
        # click on the "Next" button
        next_button.click()
    else:
        # exit the loop if there is no "Next" button
        
        year -= 1
        driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p=25&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n%5B5%2C1%5D%7CTribunal/CORTE%20SUPREMA%20DE%20JUSTICIA%20DE%20LA%20NACION%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")

# close the driver
driver.quit()

    
