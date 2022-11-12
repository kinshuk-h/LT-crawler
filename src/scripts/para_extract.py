import os
import re
import pandas
import argparse
import traceback

CSV_FIELD_NAMES = [ "Document", "Paragraph Number", "Paragraph Content" ]
DEFAULT_PARA_SEP_REGEX = re.compile(r"(?ui)^(\d+)\.\s")

def as_int(text):
    try: return int(text)
    except: return None

def segment_as_paragraphs(text_file, segmenting_regex):
    paragraphs = []
    current_paragraph = []
    current_segment = 0
    paragraph = { 'Document': os.path.basename(text_file) }
    with open(text_file, "r", encoding="utf-8") as file:
        for line in file:
            if match := segmenting_regex.search(line):
                if (segment_number := as_int(match[1])) is not None:
                    if segment_number > current_segment:
                        paragraph.update({
                            'Paragraph Number': current_segment,
                            'Paragraph Content': '\n'.join(current_paragraph).strip()
                        })
                        paragraphs.append({ **paragraph })
                        current_segment = segment_number
                        current_paragraph = []
                else:
                    paragraph.update({
                        'Paragraph Number': current_segment,
                        'Paragraph Content': '\n'.join(current_paragraph).strip()
                    })
                    paragraphs.append({ **paragraph })
                    current_segment += 1
                    current_paragraph = []
                current_paragraph.append(line.strip('\n\r'))
            elif len(paragraphs) > 1:
                current_paragraph.append(line.strip('\n\r'))
    paragraph.update({
        'Paragraph Number': current_segment,
        'Paragraph Content': '\n'.join(current_paragraph).strip()
    })
    paragraphs.append(paragraph)
    return paragraphs[1:]

def main():
    parser = argparse.ArgumentParser(
        description="Extract paragraphs from processed text files"
    )
    parser.add_argument("text_files", nargs='+', help="text files to process")
    parser.add_argument("-r", "--regex", type=re.compile, default=DEFAULT_PARA_SEP_REGEX,
                        help="regex to separate paragraphs. Defaults to " + str(DEFAULT_PARA_SEP_REGEX))
    parser.add_argument("-o", "--output-csv", default='Paragraphs.csv', help="path to destination CSV file")

    args = parser.parse_args()

    if not os.path.exists(args.output_csv):
        open(args.output_csv, 'w+').close()

    paragraph_df = pandas.read_csv(args.output_csv, names=CSV_FIELD_NAMES)

    for text_file in args.text_files:
        try:
            print("[>] processing ", text_file, " ... ", end='', sep='', flush=True)
            paragraphs = segment_as_paragraphs(text_file, args.regex)
            paragraph_df = pandas.concat([
                paragraph_df, pandas.DataFrame(paragraphs)
            ], ignore_index=True)
            print("done")
        except Exception as exc:
            print("error")
            print("[X] error:", exc)
            traceback.print_exc()

    paragraph_df.to_csv(args.output_csv, index=False)

if __name__ == "__main__":
    main()