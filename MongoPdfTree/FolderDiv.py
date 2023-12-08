import os
import re
from fpdf import FPDF
from pymongo import MongoClient
import sys

grades= ["6","7","8","9","10"]
subject=""
for grade in grades:
    filter_q={ "Grade":[grade] }

    projection={'Chapter':1,
                'Topic':1,
                'Data':1,
                '_id':0}
    cluster = MongoClient(" ") #ADD YOUR MONGO STRING HERE
    db=cluster[""]
    collection = db[f""]

    data_r=collection.find(filter_q,projection)

    data=[x for x in data_r]

    print(len(data))

    master=rf"" #LINK YOUR FILES HERE D:\VSC\Python\Scraping\PDFScraper\Masters collection\Masters {subject}\Class {grade}(eg)
    if not os.path.exists(master):
        os.makedirs(master)                         # Use os.makedirs to create super parent directories

    for item in data:
        pattern = r'[!@#$%^&*()_+={}\[\]:;"\'<>,.?/\|\\`~-–—]'
        
        chapters = item.get('Chapter', [])
        topics = item.get('Topic', [])
        datas = item.get('Data', [])

        for chapter, topic, data in zip(chapters, topics, datas):

            chapter=re.sub(pattern,' ',chapter).title().strip()
            chapter_folder = os.path.join(master, chapter)
            if not os.path.exists(chapter_folder):
                os.makedirs(chapter_folder)         # Use os.makedirs to create parent directories

        
            topic=re.sub(pattern,' ',topic).title().strip()
            # data = re.sub(r'\\u[0-9a-fA-F]{4}', ' ', data)
            subfolder = os.path.join(chapter_folder, topic)
            
            if not os.path.exists(subfolder):
                os.makedirs(subfolder)              # Use os.makedirs to create sub parent directories

            # pdf_filename = f"{topic}.pdf"
            
            pdf=FPDF()
            
            pdf.add_page()
            
            pdf.set_font("Arial",size=10)
                    
            pdf.multi_cell(200,10,txt=f'{data}',border=0,align='L')
            
            pdf_path=os.path.join(subfolder,rf'{topic}.pdf')
            
            pdf.output(pdf_path)
