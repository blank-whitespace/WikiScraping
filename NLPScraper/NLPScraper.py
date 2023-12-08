# Imports
from bs4 import BeautifulSoup
import urllib.parse 
import sys
import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}
concurr_max= 40
starting_row=0
batch_size = 40
timeout_seconds=120

topic_path=r"D:\VSC\Python\Scraping\WebScraper 3\Full cleaned\topics.txt"

# topics = {item.strip('"').replace("\n","").strip().replace('"','') for item in open(topic_path,'r').read().split(',') if item}
errorcodes=[]
err=[]
def signal_handler(signal, frame):
    # Perform cleanup operations here, if needed
    print("Program terminated by user.")
    sys.exit(0)
          
def get_def(ent):
    description = ""
    WikiHome = "https://en.wikipedia.org/wiki/"
    ent = ent.lower().strip().replace(" ", "_")
    wiki = WikiHome + ent
    
    print(wiki)

    response = requests.get(wiki)
    if response.status_code not in (200, 301, 302, 404):
        print("Failed to fetch the web page. Status Code 200")
        return ""

    html_content = response.text
    wikip = BeautifulSoup(html_content, "html.parser")
    # If page is not found
    if wikip.find("div", class_="noarticletext mw-content-ltr"):
        print("No specific page found")
        return ""

    categ = ""
   
    c = 0  # Relevancy variable
    
    # Concat all categories
    if (wikip.find("div", id="mw-normal-catlinks")):
        lis = (wikip.find("div", id="mw-normal-catlinks")).find_all("li")
        for li in lis:
            categ = categ + (li.get_text().lower()) + " "
        print(categ)
    else:
        print("No categories")
    # categtrack.write(categ)
    #Exit and write to disambiguation pages
    if categ.find("disambiguation pages") != -1:
        # disamb.writerow([ent,wiki])
        print("Disambiguation found")
        return ""
    
    # Give relevancy score
    # for word in topics:
    #     if categ.count(word) >= 2:
    #         c += categ.count(word)
    # Check if page content is relevant + get description
    # if c >= 2:
    content = wikip.find("div", class_="mw-content-ltr mw-parser-output")
    # print(content)
    if content:
        print("Page has content")
        word_count = 0
        scraped_text = ""
        
        h2_index=content.find('h2')
        
        # print("index",h2_index)
        if h2_index is not None:
            ptags= h2_index.find_all_previous('p')
            
            # listofparas = [p.get_text(strip=True) for p in ptags]
            for i,ptag in enumerate(ptags[::-1]):
                if i>0:
                    scraped_text+=' '
                for elem in ptag.contents:
                    if isinstance(elem,str):
                        scraped_text+=(elem.strip())+' '
                    else:
                        text = elem.get_text(separator=' ', strip=True)
                        scraped_text+= text + ' '
                        
        # print(scraped_text)       
        return scraped_text
    
    else: return ""

    # else:
    #     description = ""
    #     print("Page is unrelated")
    # return description.strip()

def fetch_image_details(session, img_url):
    global errorcodes, err
    try:
        image_link = ""

        response = requests.get(img_url)
        if response.status_code != 200:
            return {"url": img_url, "relevancy": 0}

        img_html = response.text
        img_soup = BeautifulSoup(img_html, "html.parser")

        # Calculate relevancy based on categories
        # categ = " ".join(li.get_text().lower() for li in img_soup.find("div", id="mw-normal-catlinks").find_all("li"))
        # relevancy = sum(1 for word in topics if word in categ)
        
        image_link = img_soup.find("div", class_="fullImageLink").find("a", href=True)['href']

        return {"url": image_link}  # , "relevancy": relevancy}

    except Exception as e:
        errorcodes.append(2)
        err.append("No image in image search link")
        return {"url": image_link}  # , "relevancy": 0}    

def get_images(ent):
    global errorcodes, err

    try:
        search_params = {
            "search": ent,
            "title": "Special:MediaSearch",
            "go": "Go",
            "type": "image"
        }

        search_url = "https://commons.wikimedia.org/w/index.php" + "?" + urllib.parse.urlencode(search_params)
        
        response = requests.get(search_url)
        if response.status_code != 200:
            print(f"Failed to fetch the web page. Status Code {response.status_code}")
            return []

        html_content = response.text
        imagepgsoup = BeautifulSoup(html_content, "html.parser")
        target_div = imagepgsoup.find_all("div", class_="sdms-search-results__list sdms-search-results__list--image")

        img_links = []

        for div in target_div:
            target_links = div.find_all("a")
            for element in target_links:
                image_url = element["href"]
                img_links.append(image_url)

        # Fetch image details one at a time
        images = []
        for img_url in img_links[0:15]:
            image_details = fetch_image_details(None, img_url)  # Replace None with any required session or additional parameters
            images.append(image_details)

        # Filter out images with relevancy score >= 2
        relevant_images = [img for img in images] # if img["relevancy"] >= 2]

        if not relevant_images:
            errorcodes.append(7)
            err.append("Irrelevant photo")

        return [img["url"] for img in relevant_images]

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []