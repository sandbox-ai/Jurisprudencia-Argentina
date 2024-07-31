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
    base_url = "http://www.saij.gob.ar/busqueda?o=0&p=1000&f=Total%7CFecha%7CTema%5B5%2C1%5D%7COrganismo%5B5%2C1%5D%7CAutor%5B5%2C1%5D%7CJurisdicci%C3%B3n%5B5%2C1%5D%7CTribunal%2FCORTE+SUPREMA+DE+JUSTICIA+DE+LA+NACION%7CPublicaci%C3%B3n%5B5%2C1%5D%7CColecci%C3%B3n+tem%C3%A1tica%5B5%2C1%5D%7CTipo+de+Documento%2FJurisprudencia&s=fecha-rango%7CDESC&v=colapsada"
    offset = 0
    all_urls = []

    while True:
        urls = get_urls(base_url, offset)
        if not urls:
            break
        all_urls.extend(urls)
        print(f"{len(urls)} URLs from offset {offset}")
        offset += 1000
        time.sleep(1)  # Be respectful to the server

    print(f"URLs para STJ: {len(all_urls)}")
    
    for url in all_urls:
        response = requests.get("http://www.saij.gob.ar/view-document?guid="+url.split("/")[-1])
        response_json = json.loads(response.content)
        # extract the content

        # Find linked content
        #JSONcontent = {"url": url, "content": contenido, "date": find_date(contenido), "attached_file":None}

        print(response_json)

    # Optionally, save the URLs to a file
    with open('collected_urls.txt', 'w') as f:
        for url in all_urls:
            f.write(f"{url}\n")

if __name__ == "__main__":
    main()