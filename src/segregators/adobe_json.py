import json
import regex

from .base import Segregator

class AdobeJSONSegregator(Segregator):
    """ Segregator to segregate paragraphs from Adobe's API JSON results """

    @classmethod
    def select(cls, files):
        for file in files:
            if file.endswith('.json'):
                return file

    @classmethod
    def load(cls, file_path):
        if file_path is None:
            return None
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)

    @classmethod
    def segregate(cls, data):
        if data is None: return
        try:
            elements = data['elements']
            current_page, para_num, page_start, valid_content = 0, 1, 0, False
            extended_para_starter_regex = regex.compile(r"(?ui)^\p{Z}*((?:\p{N}+\p{Z}*\.)+)")
            paragraph_starter_regex = regex.compile(r"(?ui)^\p{Z}*\p{N}+\p{Z}*\.")
            header_path_regex       = regex.compile(r"(?u)\/H\d+")
            content = []

            for element in elements:
                if 'Text' in element:
                    if 'Table' in element['Path']:
                        # TODO: Decide how to deal with text elements from tables.
                        continue
                    is_paragraph_starter = paragraph_starter_regex.match(element['Text'])
                    is_heading           = header_path_regex.search(element['Path'])
                    if element['Page'] != current_page and (is_heading or is_paragraph_starter):
                        current_page = element['Page']
                    if is_paragraph_starter:
                        if len(content) > 0:
                            para_ref = None
                            if match := extended_para_starter_regex.search(content[0]):
                                para_ref = para_ref = regex.sub(r"(?ui)\p{Z}+", "", match[1])
                                content[0] = content[0][:match.start()] + content[0][match.end():]
                            yield {
                                # Add +1 to page, as Adobe JSON result uses 0-based indexing.
                                'page': page_start + 1,
                                'paragraph_number': para_num,
                                'content': ' '.join(content).strip(),
                                'reference': para_ref
                            }
                            para_num += 1
                        if not valid_content:
                            valid_content = True
                        content.clear()
                        page_start = current_page
                    if not is_heading and valid_content:
                        content.append(element['Text'])
        except:
            return
