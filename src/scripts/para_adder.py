import json

LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
JUDGMENT_FILE = "H:\All Python Files\Projects\Legal Text Analysis\Judgments\DHC Judgments\judgments 2022-09-23T00-51-22.json"

if __name__ == "__main__":
    judgments = None
    with open(JUDGMENT_FILE, "r+", encoding="utf-8") as file:
        judgments = json.load(file)
    for document in judgments['data']:
        document['paragraphs'] = [
            {
                "content": LOREM_IPSUM,
                "page": (para_num // 3) + 1,
                "paragraph_number": para_num + 1
            }
            for para_num in range(10 * 3)
        ]
    # print(json.dumps(judgments['data'][0], ensure_ascii=False, indent=4), sep='\n')
    with open(JUDGMENT_FILE, "w+", encoding="utf-8") as file:
        json.dump(judgments, file, ensure_ascii=False, indent=4)