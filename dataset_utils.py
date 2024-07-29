import os
import datetime
import re

def calculate_tokens(directory):
    import tiktoken
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from langchain.document_loaders import TextLoader
    from langchain.vectorstores.faiss import FAISS
    from langchain.document_loaders import DirectoryLoader
    if not directory:
        loader = DirectoryLoader('./', glob="**/*.json")
    else:
        loader = DirectoryLoader(directory, glob="**/*.json", loader_cls=TextLoader)

    docs = loader.load()
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
    )
    documents = text_splitter.split_documents(docs)
    encoding = tiktoken.encoding_for_model('gpt-3.5-turbo')
    return len(encoding.encode(documents))

def replace_tilde_chars(input_string):
    """
    Replaces tilde characters with their corresponding non-tilde characters.

    Args:
        input_string: A string containing tilde characters.

    Returns:
        A new string with all tilde characters replaced with their corresponding non-tilde characters.
    """
    tilde_dict = {
        'ã': 'a', 'ẽ': 'e', 'ĩ': 'i', 'õ': 'o', 'ũ': 'u',
        'Ã': 'A', 'Ẽ': 'E', 'Ĩ': 'I', 'Õ': 'O', 'Ũ': 'U',
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N'
    }
    output_string = ''
    for char in input_string:
        if char in tilde_dict:
            output_string += tilde_dict[char]
        else:
            output_string += char
    return output_string

def trim_filename(filename):
    """ 
    Trim the filename if it's too long to save.
    """
    # Get the maximum filename length for the current operating system
    max_filename_length = 230

    # Check if the filename is longer than the maximum length
    if len(filename) > max_filename_length:
        # Calculate the maximum length for the filename without the extension
        filename_without_ext, ext = os.path.splitext(filename)
        max_length_without_ext = max_filename_length - len(ext)

        # Trim the filename to the maximum length
        trimmed_filename = filename_without_ext[:max_length_without_ext] + ext
        return trimmed_filename
    else:
        return filename

def clean_filename(filename):
    """
    Remove all conflicting characters and duplicate "-" from the filename.
    """
    filename = filename.split('.txt')[0]
    conflicting_dict={"/":"-" , '"':"" , ':':"-" , 'º':"" , '?':"" , '*':"-" , ":":"-" , "(":"" , ")":"" , ".":"" , " ":"-","<":"", "'":""}
    unconflicted = ''
    output_string=''
    prev_char=''
    for char in filename:
        if char in conflicting_dict:
            unconflicted += conflicting_dict[char]
        else:
            unconflicted += char
    unconflicted = unconflicted.replace("[[p]]","").replace("[[-p]]","")
    return unconflicted+".txt"

def find_date(content):
    match = re.search(r"\b(\d{1,2})\s+(?:d[eel]\s+)?([^\W\d_]+)\s+(?:d[eel]\s+)?(\d{4})", content, re.IGNORECASE)
    if match:
        day = int(match.group(1))
        month_name = match.group(2).lower()
        year = int(match.group(3))
        month_names = ["enero", "febrero", "marzo", "abril", "mayo", "junio", 
                    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
        try:
            month = int(month_names.index(month_name) + 1)
        except ValueError:
            print("Could not find a valid month in input string")
            return "datenotfound"
        else:
            date_str = f"{year}-{month:02d}-{day:02d}"
            date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            formatted_date_str = date_obj.strftime("%Y-%m-%d")
            return formatted_date_str
    else:
        print("Could not find a valid date in input string")
        return "datenotfound"