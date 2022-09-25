import json
from extractors import AdobeAPIExtractor

if __name__ == "__main__":
    with open("structuredData.json", "r+", encoding='utf-8') as json_file:
        data = json.load(json_file)
        AdobeAPIExtractor.save_as_text(data, "structuredData.txt")
        AdobeAPIExtractor.save_as_text(data, "structuredData_processed.txt", format_wrt_layout=True)