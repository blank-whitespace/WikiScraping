# Imports
from bs4 import BeautifulSoup
import csv
import urllib.parse 
from concurrent.futures import ThreadPoolExecutor
import logging
import signal
import sys
import math
import asyncio
import aiohttp
from pymongo import MongoClient

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}
concurr_max= 40
starting_row=0
batch_size = 40
timeout_seconds=120

topic_path=r"D:\VSC\Python\Scraping\WebScraper 3\Full cleaned\topics.txt"

topics = {item.strip('"').replace("\n","").strip().replace('"','') for item in open(topic_path,'r').read().split(',') if item}
errorcodes=[]
err=[]
def signal_handler(signal, frame):
    # Perform cleanup operations here, if needed
    print("Program terminated by user.")
    sys.exit(0)

async def scrape_entity(entity, link,writer,rewriter,semaphore,errorcodes,err):
    definition=""
    images=[]
    try:
        async with semaphore:
            definition = await get_definition(link)
            images = await get_images(entity)
            if definition!= "" and definition is not None:
                if images:
                    print(entity,"Worked")
                    for image in images:
                        writer.writerow([entity, definition, image])
                else:
                    print(entity,link,"fail")
                    errorcodes.append(1)
                    err.append("No Images in search")
                    rewriter.writerow([entity,definition,link,errorcodes,err])
            else:
                errorcodes.append(11)
                err.append("No definition")
                rewriter.writerow([entity,definition,link,errorcodes,err])
    except Exception as e:
        errorcodes.append(5)
        err.append(f"Logging error: {e}")
        rewriter.writerow([entity,definition,link,errorcodes,err])
        logging.error(f"Error processing {entity}: {str(e)}")       
        
async def get_definition(entity):
    global errorcodes,err
    link=f'https://en.wikipedia.org/wiki/{entity}'
    async with aiohttp.ClientSession() as session:
        async with session.get(link, timeout=timeout_seconds) as response:
            if response.status not in (200, 301, 302, 404):
                print("Failed to fetch the web page. Status Code 200")
                errorcodes.append(8)
                err.append(f"Failed to fetch webpage:{response.status}")
                return ""

            html_content = await response.text()
            link = BeautifulSoup(html_content, "html.parser")
            
            # If page is not found
            if link.find("div", class_="noarticletext mw-content-ltr"):
                print("No specific page found")
                errorcodes.append(9)
                err.append(f"Failed to fetch webpage:{response.status}")
                return ""

            categ = ""
            c = 0  # Relevancy variable
            
            # Concat all categories
            lis = (link.find("div", id="mw-normal-catlinks")).find_all("li")
            for li in lis:
                categ += (li.get_text().lower()) +" "

            # Give relevancy score
            for word in topics:
                if categ.find(word):
                    c += 1

            # Check if page content is relevant + get description
            if c >= 2:
                print("Page has content")
                content = link.find("div", id="mw-content-text")
                word_count = 0
                word_bottom=500
                description=""

                for para in content.find_all("p"):
                    if para.find_all("sup"):
                        for sup_tag in para.find_all("sup"):
                            sup_tag.extract()

                    if para.find_all("a"):
                        for link in para.find_all("a"):
                            link.replace_with(link.get_text())

                    if para.find(["h1", "h2", "h3", "h4", "h5", "h6"]):
                        description += "\n"

                    para_text = para.get_text().strip() + " "
                    current_word_count = len(para_text.split())
    
                    if word_bottom <= word_count + current_word_count <= 3000:  # Limit to 2000-3000 words
                        word_count += current_word_count
                        
                    elif word_count + current_word_count < word_bottom:
                        word_count += current_word_count
                        description += para_text
                    else:
                        description += para_text
                        break
                if word_count<word_bottom:
                    print(f"Description not long enough:{word_count}")
                    errorcodes.append(3)
                    err.append("Definition too short")
                    return ""
                # print(description,"DESCRIPTION")        
                return description.strip()

            else:
                description = ""
                print("Page is unrelated")
                errorcodes.append(6)
                err.append("Irrelevant Page")
            return description.strip()

async def fetch_image_details(session, img_url):
    global errorcodes,err
    try:
        image_link=""
        async with session.get(img_url) as response:
            if response.status != 200:
                
                return {"url": img_url, "relevancy": 0}

            img_html = await response.text()
            img_soup = BeautifulSoup(img_html, "html.parser")

            # Calculate relevancy based on categories
            categ = " ".join(li.get_text().lower() for li in img_soup.find("div", id="mw-normal-catlinks").find_all("li"))
            relevancy = sum(1 for word in topics if word in categ)
            
            image_link = img_soup.find("div", class_="fullImageLink").find("a", href=True)['href']
            # print(image_link)

            return {"url": image_link, "relevancy": relevancy}
    except Exception as e:
        errorcodes.append(2)
        err.append("No image in image search link")
        return {"url": image_link, "relevancy": 0}
        
async def get_images(ent):
    global errorcodes,err
    try:
        
        search_params = {
            "search": ent,
            "title": "Special:MediaSearch",
            "go": "Go",
            "type": "image"
        }

        search_url = "https://commons.wikimedia.org/w/index.php" + "?" + urllib.parse.urlencode(search_params)

        async with aiohttp.ClientSession() as session:
            async with session.get(search_url) as response:
                if response.status != 200:
                    print(f"Failed to fetch the web page. Status Code {response.status}")
                    return []

                html_content = await response.text()
                imagepgsoup = BeautifulSoup(html_content, "html.parser")
                target_div = imagepgsoup.find_all("div", class_="sdms-search-results__list sdms-search-results__list--image")

                img_links = []

                for div in target_div:
                    target_links = div.find_all("a")
                    for element in target_links:
                        image_url = element["href"]
                        img_links.append(image_url)

                # Now you can fetch image details concurrently
                tasks = [fetch_image_details(session, img_url) for img_url in img_links]
                # Use asyncio.wait_for to set a timeout for waiting for images
                images = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout_seconds)

                # Filter out images with relevancy score >= 2
                relevant_images = [img for img in images if img["relevancy"] >= 2]
                
                if not relevant_images:
                    errorcodes.append(7)
                    err.append("Irrelevant photo")
                
                return [img["url"] for img in relevant_images]
            
    except asyncio.TimeoutError:
        print("Timeout: The image scraping process took too long.")
        errorcodes.append(4)
        err.append("Image extraction timeout")
        return []
    
    except Exception as e:
        print(e)
        print("Exception in img search")
        errorcodes.append(10)
        err.append(f"Image extraction error: {e}")
        return []
    
async def main():
    global errorcodes,err
    async with aiohttp.ClientSession() as session:
        semaphore=asyncio.Semaphore(concurr_max)
    with open(r"D:\VSC\Python\Scraping\WebScraper 3\\Non-Disambiguation2.csv", "r", newline="", encoding="utf-8-sig") as data_csv, \
        open(r"D:\VSC\Python\Scraping\WebScraper 3\async.csv", "w", newline="", encoding="utf-8") as dataprocessed, \
        open(r"D:\\VSC\\Python\\Scraping\\WebScraper 3\\nodata.csv","w",newline="",encoding="utf-8") as dataunprocessed:

        reader = csv.reader(data_csv)
        writer = csv.writer(dataprocessed)
        rewriter = csv.writer(dataunprocessed)
        
        # Header Rows
        writer.writerow(["Entity", "Description", "Images"])
        rewriter.writerow(["Entity", "WikiCommonsLink","Error Codes","Error"])
        
        # Load entities and links
        entities = []
        links = []


        for row_number,row in enumerate(reader):
            if row_number < starting_row:
                continue
            if row:
                entities.append(row[1])
                links.append(row[2])
        
        
        num_batches = math.ceil(len(entities) / batch_size)
        
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(entities))
        
            batch_tasks=[]
            
            # Process entities using a thread pool
            for idx in range(start_idx, end_idx):
                errorcodes.clear()
                err.clear() 
                entity = entities[idx]
                link = links[idx]
                batch_tasks.append(scrape_entity(entity,link,writer,rewriter,semaphore,errorcodes,err))
        
            await asyncio.gather(*batch_tasks)

def getMongoData():
    cluster = MongoClient("mongodb://yozu:yozupass123@3.108.93.82:27017/?authMechanism=DEFAULT&authSource=admin")
    db=cluster["YoZu"]
    collection = db["NCERT_Science_data"]

    query_result = collection.find({},{"_id":0,"Data":1})
    
    data_values = [document["Data"] for document in query_result]
    return data_values
          
if __name__ == "__main__":
    # Set up a signal handler for KeyboardInterrupt (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())