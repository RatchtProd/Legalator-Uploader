import pdfplumber
from tabulate import tabulate
from tqdm.auto import tqdm  # this is our progress bar
import os
from files.splitter import RecursiveCharacterTextSplitter

def check_bboxes(word, table_bbox):
  """
  Check whether word is inside a table bbox.
  """
  l = word['x0'], word['top'], word['x1'], word['bottom']
  r = table_bbox
  return l[0] > r[0] and l[1] > r[1] and l[2] < r[2] and l[3] < r[3]

def join_page(lines: list) -> str:
  page = ""
  for line in lines:
    if type(line) == list:
      new_list = []
      for row in line:
        row = [ cell.replace("\n", " ") for cell in row if cell != None]
        new_list.append(" ".join(row))
      page += ("\n".join(new_list)) 
      continue
    page += (line + " ")
  
  return page

def process_page(file_name, page):
  # check for tables
  tables = page.find_tables()
  table_bboxes = [i.bbox for i in tables]
  tables = [{'table': i.extract(), 'top': i.bbox[1]} for i in tables]

  # get all words that are not in a table
  non_table_words = [word for word in page.extract_words() if not any(
      [check_bboxes(word, table_bbox) for table_bbox in table_bboxes])]
  

  lines = []
  for cluster in pdfplumber.utils.cluster_objects(non_table_words + tables, 'top', tolerance=5):
    if 'text' in cluster[0]:
      lines.append(' '.join([i['text'] for i in cluster]))
    elif 'table' in cluster[0]:
      lines.append(cluster[0]['table'])
  
  print("PAGE PROCESSED:\n\n")
  page_processed = f"Title: {file_name}\n\n{join_page(lines)}"
  print(page_processed)
  
  return page_processed

def read_pdf(chapter_path: str, folder_dir):
  chunks = []
  with pdfplumber.open(f"{folder_dir}/{chapter_path}") as pdf:
    print(f"File: {chapter_path}")
    chapter_name = os.path.splitext(chapter_path)[0]
    print(chapter_path)
    for i in tqdm(range(0, len(pdf.pages)), ncols=50):
      page = process_page(chapter_path, pdf.pages[i])
      if page != "": chunks.append(page)

  return chunks


def read_txt(file_path: str, folder_dir):
  with open(f"{folder_dir}/"+file_path, "r") as file:
    body = file.read()
    text_splitter = RecursiveCharacterTextSplitter()

    chunks = text_splitter.split_text(body)
    return chunks