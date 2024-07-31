import requests, json
from bs4 import BeautifulSoup
import time
from dataset_utils import trim_filename,replace_tilde_chars,clean_filename,find_date

def get_urls(base_url, offset):
    url = base_url.replace("o=0", f"o={offset}")
    response = requests.get(url)
    j=json.loads(response.text)
    urls=[]
    try:
        for results in j["searchResults"]["documentResultList"]:
            result = json.loads(results["documentAbstract"])
            url = result["document"]["metadata"]["friendly-url"]["description"]+"/"+result["document"]["metadata"]["uuid"]
            urls.append(url)
        return urls
    except:
        return None

def main():
    base_url = "http://www.saij.gob.ar/busqueda?o=0&p=1000&f=Total|Fecha/{}[20,1]|Estado de Vigencia[5,1]|Tema[5,1]|Organismo[5,1]|Autor[5,1]|Jurisdicción|Tribunal[5,1]|Publicación[5,1]|Colección temática[5,1]|Tipo de Documento/Jurisprudencia&s=fecha-rango|DESC&v=colapsada"
    offset = 0
    year=2024
    all_urls = []

    while year >= 1800:
        offset = 0  # reset offset for each year
        while True:
            urls = get_urls(base_url.format(year), offset)
            if not urls:
                break
            all_urls.extend(urls)
            print(f"{len(urls)} URLs from offset {offset}")
            offset += 1000
            time.sleep(1)  # Be respectful to the server
        print(f"Collected {len(all_urls)} URLs for year {year}")
        year -= 1  # move to the previous year


    print(f"URLs: {len(all_urls)}")

    with open('urls.txt', 'a') as f:
        for url in all_urls:
            f.write(f"{url}\n")

    for url in all_urls:
        response = requests.get("http://www.saij.gob.ar/view-document?guid="+url.split("/")[-1])
        response_json = json.loads(response.content)
        # extract the content
        contenido = json.loads(response_json['data'])['document']['content']
        # Find linked content
        #JSONcontent = {"url": url, "content": contenido}

        print(contenido)

        with open('dataset.jsonl', 'a') as f:
            f.write(f"{contenido}\n")

if __name__ == "__main__":
    main()