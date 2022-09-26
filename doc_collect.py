import os
import re
import sys
import glob
import json
import logging
import datetime
import argparse
import traceback

import retrievers
import extractors

if '-d' in sys.argv or '--debug' in sys.argv:
    logging.basicConfig(level=logging.CRITICAL)
    retrievers.logger.setLevel(logging.DEBUG)
    extractors.logger.setLevel(logging.DEBUG)

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
    'pdfminer_text': extractors.PdfminerHighLevelTextExtractor(),
    'parsr': extractors.ParsrExtractor(),
    'parsr_custom': None,
    'adobe_api': extractors.AdobeAPIExtractor(
        credentials_file=os.path.join(
            "credentials", "pdfservices-api-credentials.json"
        )
    )
}

def main():
    parser = argparse.ArgumentParser(
        description="Collect and extract text from judgments from SC and DHC websites"
    )
    parser.add_argument('query', help='text to search for judgments', nargs='?', default='')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='enable debug logs')
    parser.add_argument('-c', '--court', default='DHC', choices=[ *avl_retrievers.keys() ],
                        help='court website to use to scrape judgments')
    parser.add_argument('-e', '--extractor', nargs='*', default=[ *avl_extractors.keys() ],
                        choices=[ *avl_extractors.keys() ],
                        help='extractor(s) to use for mining content from the judgment')
    parser.add_argument('-p', '--page', type=int, default=0,
                        help='page number to fetch information from, defaulting to the first')
    parser.add_argument('--start-date', type=datetime.date.fromisoformat, help='load judgments from this date',
                        default=None, nargs='?')
    parser.add_argument('--end-date', type=datetime.date.fromisoformat, help='load judgments upto this date',
                        default=None, nargs='?')
    parser.add_argument('--parsr-config', default=None, help='path to configuration for parsr')
    parser.add_argument('-o', '--output-dir', default='Judgments', help='output directory to store judgments')
    parser.add_argument('-J', '--omit-json', action='store_false', dest='save_json',
                        help='omit saving judgment list results as JSON')
    parser.add_argument('--skip-existing', action='store_true', help='skip operations if results already exist')

    args = parser.parse_args()

    if args.parsr_config is not None:
        with open(args.parsr_config, 'r+', encoding='utf-8') as config:
            avl_extractors['parsr_custom'] = extractors.ParsrExtractor(json.load(config))
    else:
        args.extractor.remove('parsr_custom')

    if not args.query and args.court != 'SC':
        print(parser.prog, ": error: query cannot be empty", sep='', file=sys.stderr, flush=True)
        sys.exit(1)

    output_dir = os.path.join(args.output_dir, f"{args.court} Judgments")
    os.makedirs(output_dir, exist_ok=True)

    retriever = avl_retrievers[args.court]

    try:
        if args.skip_existing and (judgment_files := glob.glob("*.pdf", root_dir=output_dir)):
            print(parser.prog, ": skipping search and downloading judgments (files exist)", sep='', flush=True)
            judgment_files = [ os.path.join(output_dir, file) for file in judgment_files ]
        else:
            print(parser.prog, ': searching judgments from ', args.court, ' ... ', sep='', end='', flush=True)
            judgments, metadata = retriever.get_judgments(
                args.query, page=args.page,
                start_date=args.start_date, end_date=args.end_date
            )

            if not judgments:
                print('error', flush=True)
                print(parser.prog, ": error: no judgments found", sep='', file=sys.stderr, flush=True)
                sys.exit(1)
            else: print('done', flush=True)

            print(parser.prog, ': downloading judgments to "', output_dir, '" ... ', sep='', end='', flush=True)
            judgment_files = retriever.save_documents(judgments, output_dir=output_dir)
            print('done', flush=True)

            if args.save_json:
                json_file = f'judgments {pathsafe(now())}.json'
                json_file_path = os.path.join(output_dir, json_file)
                print(parser.prog, ': saving judgment search results to ', json_file, ' ... ', sep='', end='', flush=True)
                result = {
                    'meta': {
                        'directory': os.path.abspath(output_dir),
                        'request': {
                            'query'     : args.query or "",
                            'court'     : args.court,
                            'page'      : args.page or 0,
                            'start_date': args.start_date,
                            'end_date'  : args.end_date
                        },
                    },
                    'data': judgments
                }
                if metadata is not None:
                    result['meta']['response'] = metadata
                with open(json_file_path, 'w+', encoding='utf-8') as file:
                    json.dump(result, file, indent=4, ensure_ascii=False)
                print('done', flush=True)

        for extrctr in args.extractor:
            extractor = avl_extractors[extrctr]
            extract_output_dir = os.path.join(output_dir, pathsafe("extracted_" + extrctr))
            os.makedirs(extract_output_dir, exist_ok=True)
            print(
                parser.prog, ': extracting content from downloaded judgments using ',
                extrctr, ' ... ', sep='', end='', flush=True
            )
            for pdf_file in judgment_files:
                try:
                    extractor.extract_to_file(
                        pdf_file, output_dir=extract_output_dir,
                        skip_existing=args.skip_existing
                    )
                except Exception as exc:
                    print('\n  error:', exc, ' ... ', sep='', end='')
            print('done', flush=True)
    except Exception as exc:
        print('error', flush=True)
        print(parser.prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()