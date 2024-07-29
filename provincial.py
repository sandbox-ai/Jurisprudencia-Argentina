import os
import re
import datetime
import glob
import json
import hashlib
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dataset_utils import trim_filename,replace_tilde_chars,clean_filename,find_date
import argparse

year = int(datetime.datetime.now().year)

parser = argparse.ArgumentParser(usage="EN Scrap court veredicts from SAIJ. Supreme court, federal, national and provincial juridisprudence\nES Scrapear fallos de SAIJ. Jurisprudencia de la corte suprema, federal, nacional y provincial")
parser.add_argument('--jurisprudencia', required=True, type=str, help='corte-suprema, federal, nacional, provincial')
parser.add_argument('--year', type=int, help='Scrap starts from a specific year', default=year)
parser.add_argument('--results_per_page', type=int, help='How many results per page loaded by SAIJ', default=100)
parser.add_argument('--stop', type=int, help='What year to stop scrapping', default=None)
parser.add_argument('--log', type=str, help='Set log level to INFO|WARNING|ERROR', default=None)
parser.add_argument('--output', type=str, help='Directory to write results to. Default is ./dataset', default="dataset")
args = parser.parse_args()

# set logging level
loglevel = args.log
numeric_level = getattr(logging, loglevel.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)
logging.basicConfig(level=numeric_level)

# create the output directory if it doesn't exist
if not os.path.exists(args.output+"/jurisprudencia-"+args.jurisprudencia):
    os.mkdir(args.output+"/jurisprudencia-"+args.jurisprudencia)

# set up the Chrome driver
options = Options()
options.headless = False
driver = webdriver.Chrome(options=options)
# set filter by year because the page cant show entries beyond 100.000 results
year= args.year
# set how many results per page. 100 is a safe value to avoid timeouts
results_per_page = args.results_per_page
# navigate to the website. This should start with http://www.saij.gob.ar/resultados.jsp
match args.jurisprudencia:
    case'provincial':
        driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n/Local%7CTribunal%5B5%2C1%5D%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
    case'federal':
        driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n/Federal%7CTribunal%5B5%2C1%5D%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
    case'nacional':
        driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n/Nacional%7CTribunal%5B5%2C1%5D%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
    case'corte-suprema':
        driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n%5B5%2C1%5D%7CTribunal/CORTE%20SUPREMA%20DE%20JUSTICIA%20DE%20LA%20NACION%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
    case other:
        raise ValueError("Jurisprudencia must be of the following: corte-suprema, federal, naciona, provincial")
existent=set(os.listdir("./dataset/jurisprudencia-"+args.jurisprudencia))

while True:
    logging.info(f"Navigating into {driver.current_url}")
    # wait for the page to load
    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "result-item")))

    # get progress to display
    total = int(driver.find_elements(By.CSS_SELECTOR, ".t-total")[0].text)
    progress = int(driver.find_elements(By.CSS_SELECTOR, ".t-primer-resultado")[0].text)
    progress = int(progress*100/total)
    logging.info(f"Scrapping {year}: {progress} %")

    # extract the result-items
    result_items = driver.find_elements(By.CSS_SELECTOR, ".result-item:not([id='vista-documento'])")

    # iterate over the result-items and extract the name and link
    for item in result_items:
        try:
            link_element = item.find_element(By.TAG_NAME, "a")
        except:
            driver.refresh()
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "result-item")))
            link_element = item.find_element(By.TAG_NAME, "a")
        
        link = link_element.get_attribute("href")
        URLhash = str(hashlib.sha1(link.split("123456789")[0].encode('utf-8')).hexdigest())
        filename = os.path.join("dataset/jurisprudencia-"+args.jurisprudencia, URLhash)
        existent=set(os.listdir("./dataset/jurisprudencia-"+args.jurisprudencia))

        file_exists=False
        # check if the file has already been scrapped, if so, remove it from the already existing list to shorten search times
        for file in existent:
            if URLhash in file:
                logging.info(f"{URLhash} already exists. Skipping")
                existent.remove(file) 
                file_exists=True
                break
        
        if not file_exists:
            # navigate to the contents
            driver.get(link)
            # wait for the content to load
            try:
                wait.until(EC.presence_of_element_located((By.ID, "texto")))
            except Exception as e:
                logging.error(f"Exited with error {e}")
                break
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

            if fallos_a_los_que_aplica:
                fallos_a_los_que_aplica_link= fallos_a_los_que_aplica.find('a')
                if fallos_a_los_que_aplica_link:
                    content+= '\nLink a fallo al que aplica: ' + fallos_a_los_que_aplica_link.get('href')
            
            if pdf_link:
                link_str = pdf_link.get('href')
                JSONcontent['attached_file']=link_str
                content+= '\nContenido adjunto: http://www.saij.gob.ar' + link_str

            # save the content to a JSON
            with open(filename+".json", "w", encoding="utf-8") as f:
                f.write(json.dumps(JSONcontent))
                logging.info(f'{URLhash} saved')
            driver.back()

    # check if there is a "Next" button
    next_button = driver.find_element(By.ID, "paginador-boton-siguiente")
    if next_button.is_displayed():
        # click on the "Next" button
        next_button.click()
    else:
        # if there is no "Next" button, go to previous year or finish
        if args.stop:
            if args.stop <= year:
                year -= 1
                attempts = 0
                while attempts<3:
                    try:
                        match args.jurisprudencia:
                            case'provincial':
                                driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n/Local%7CTribunal%5B5%2C1%5D%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
                            case'federal':
                                driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n/Federal%7CTribunal%5B5%2C1%5D%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
                            case'nacional':
                                driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n/Nacional%7CTribunal%5B5%2C1%5D%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
                            case'corte-suprema':
                                driver.get("http://www.saij.gob.ar/resultados.jsp?o=0&p="+str(results_per_page)+"&f=Total%7CFecha/"+str(year)+"%5B20%2C1%5D%7CEstado%20de%20Vigencia%5B5%2C1%5D%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%F3n%5B5%2C1%5D%7CTribunal/CORTE%20SUPREMA%20DE%20JUSTICIA%20DE%20LA%20NACION%7CPublicaci%F3n%5B5%2C1%5D%7CColecci%F3n%20tem%E1tica%5B5%2C1%5D%7CTipo%20de%20Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada")
                        break
                    except:
                        logging.error("Error. Retrying...")
                        attempts += 1
                else:
                    logging.error("Failed to get to SAIJ after 3 attempts")
            else:
                logging.info(f"Finished up to {args.stop}")
                break
        else:
            logging.info(f"Finished {year}")
            break

# close the driver
driver.quit()