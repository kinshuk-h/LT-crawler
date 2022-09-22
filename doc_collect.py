import os
import re
import sys
import json
import logging
import datetime
import argparse

import retrievers
import extractors

def now(tz=None):
    """ Returns the current timestamp in ISO 8601 format as a string. """
    timestamp = datetime.datetime.now(tz=tz).isoformat()
    return timestamp[:timestamp.rfind('.')]

def pathsafe(filename):
    """ Returns a santized, path-safe version of a filename. """
    return re.sub(r'[:/\\|*]', '-', re.sub(r'[?\"<>]', '', filename))

avl_retrievers = {
    'DHC': retrievers.DHCJudgmentRetriever,
    'SC' : retrievers.SCJudgmentRetriever
}
avl_extractors = {
    'pdfminer_text': extractors.PdfminerHighLevelTextExtractor()
}

def main():
    parser = argparse.ArgumentParser(
        description="Collect and extract text from judgments from SC and DHC websites"
    )
    parser.add_argument('query', help='text to search for judgments')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='enable debug logs')
    parser.add_argument('-c', '--court', default='DHC', choices=[ *avl_retrievers.keys() ],
                        help='court website to use to scrape judgments')
    parser.add_argument('-e', '--extractor', default='pdfminer_text', choices=[ *avl_extractors.keys() ],
                        help='court website to use to scrape judgments')
    parser.add_argument('-p', '--page', type=int, default=0,
                        help='page number to fetch information from, defaulting to the first')
    parser.add_argument('-o', '--output-dir', default='.', help='output directory to store judgments')
    parser.add_argument('-J', '--omit-json', action='store_false', dest='save_json',
                        help='omit saving judgment list results as JSON')

    args = parser.parse_args()

    if args.debug:
        retrievers.logger.setLevel(logging.DEBUG)
        extractors.logger.setLevel(logging.DEBUG)

    if not args.query:
        print(parser.prog, ": error: query cannot be empty", sep='', file=sys.stderr)
        sys.exit(1)

    output_dir = os.path.join(args.output_dir, f"{args.court} Judgments")
    os.makedirs(args.output_dir, exist_ok=True)

    retriever = avl_retrievers[args.court]
    extractor = avl_extractors[args.extractor]

    try:
        print(parser.prog, ': searching judgments from ', args.court, ' ... ', sep='', end='')
        judgments, metadata = retriever.get_judgments(args.query, page=args.page)

        if not judgments:
            print('error')
            print(parser.prog, ": error: no judgments found", sep='', file=sys.stderr)
            sys.exit(1)

        print(parser.prog, ': downloading judgments to "', output_dir, '" ... ', sep='')
        judgment_files = retriever.save_documents(judgments, output_dir=output_dir)
        print('done')

        if args.save_json:
            json_file = f'judgments {pathsafe(now())}.json'
            json_file_path = os.path.join(output_dir, json_file)
            print(parser.prog, ': saving judgment search results to ', json_file, ' ... ', sep='', end='')
            result = {
                'meta': {
                    'directory': os.path.abspath(output_dir),
                    'request': {
                        'query': args.query or "",
                        'court': args.court,
                        'page': args.page or 0,
                    },
                },
                'data': judgments
            }
            if metadata is not None:
                result['meta']['response'] = metadata
            with open(json_file_path, 'w+', encoding='utf-8') as file:
                json.dump(result, file, indent=4, ensure_ascii=False)
            print('done')

        print(parser.prog, ': extracting content from downloaded judgments using ', args.extractor, ' ... ', sep='')
        for pdf_file in judgment_files:
            extractor.extract_to_file(pdf_file)
        print('done')
    except Exception as exc:
        print('error')
        print(parser.prog, ": error: ", exc, sep='', file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()