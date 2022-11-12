import sys
import json
import asyncio
import argparse
import datetime
import traceback

import bs4
import regex
import aiohttp
import requests
import sqlalchemy

__BASE_URL__ = "https://main.sci.gov.in"

COMMA_SEPARATION = regex.compile(r"(?ui)\p{Z}*,\p{Z}*")

ENDPOINTS = {
    'captcha': f"{__BASE_URL__}/php/captcha_num.php",
    'judgements_by_date': f"{__BASE_URL__}/php/v_judgments/getJBJ.php"
}

def format_date(date_object: datetime.date):
    return '-'.join(date_object.isoformat().split('-')[::-1])

def generate_captcha():
    response = requests.get(ENDPOINTS['captcha'])
    response.raise_for_status()
    return int(response.text)

"""

    var JBJfrom_date = $('#JBJfrom_date').val(), JBJto_date = $('#JBJto_date').val(), jorrop = $('#jorrop').val(), capcthaAns = $('#ansCaptcha').val();

                if ($.trim(capcthaAns) == "") {
                    alert("Please Fill Captcha");
                    $("#ansCaptcha").focus();
                    return false;
                }
    var capcthaAns = $('#ansCaptcha').val();
                var x = document.getElementById("myAudio");
                x=x.src;
                var y=capcthaAns+".wav";          

                if (!x.includes(y))
                {
                    $('#ansCaptcha').val('');
                    alert("Invalid Captcha");
                    return false;
                }

                var oneDay = 24*60*60*1000, d1 = JBJfrom_date.split("-"), d2 = JBJto_date.split("-"), fromd = new Date(d1[2], parseInt(d1[1])-1, d1[0]), tod   = new Date(d2[2], parseInt(d2[1])-1, d2[0]);

                if(tod < fromd){
                    alert('Second date is less or equal to First date!');
                    return;
                }
                var diffDays = Math.round(Math.abs((fromd.getTime() - tod.getTime())/(oneDay)));
                    if(diffDays>365){
                    alert('Difference of dates must be less than 365 days!');
                    return;   
                }
                $("#JBJ").html("<center><img src=\"/php/img/load.gif\" alt=\"Loading...\" title=\"Loading...\" /></center>");

                $.ajax ({
                    url: '/php/v_judgments/getJBJ.php',
                    type: "POST",
                    data: {JBJfrom_date: JBJfrom_date, JBJto_date: JBJto_date, jorrop: jorrop,ansCaptcha:capcthaAns},
                    cache: false,
                    success: function (r) {
                        debugger;
                        if(r=='invalid_key') {
                            alert('Invalid Captcha!');
                            $("#JBJ").html(' ');
                        }else {
                            $("#JBJ").html(r);
                        }
                    },
                    error: function () {
                        alert('Server busy, try later!');
                        $('#ansCaptcha').val('');
                        document.getElementById('captcha').src='/php/captcha.php?'+Math.random(); 
                    },
                    complete: function(){                            
                        $('#ansCaptcha').val('');
                        showHint(); 
                        //document.getElementById('captcha').src='/php/captcha.php?'+Math.random(); 
                    }
                });
            });

"""

def get_judgements_by_date(start_date: datetime.date, end_date: datetime.date, captcha=None, debug=False):
    if captcha is None: captcha = generate_captcha()
    response = requests.post(ENDPOINTS['judgements_by_date'], {
        'ansCaptcha': captcha,
        'jorrop': 'J',
        'JBJfrom_date': format_date(start_date),
        'JBJto_date': format_date(end_date)
    })
    response.raise_for_status()
    page = bs4.BeautifulSoup(response.text, features='lxml')
    judgments = []
    if table := page.find('table'):
        for row in table('tr', style=False, recursive=False):
            cells = row('td', recursive=False)
            if len(cells) < 2: continue
            diary_number = str(cells[2].string).strip()

            if debug:
                print('\r', cells[0].string, end='')

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            case_number = str(cells[1].string).strip()
            judgment_url = cells[2]('a', recursive=False)[-1]['href']

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            petitioner_name = str(cells[1].string).strip()

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            respondent_name = str(cells[1].string).strip()

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            petitioner_advocate = str(cells[1].string).strip()

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            respondent_advocate = str(cells[1].string).strip()

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            bench = COMMA_SEPARATION.split(str(cells[1].string))

            row   = row.find_next_sibling('tr')
            cells = row('td', recursive=False)
            judgment_by = str(cells[1].string).strip()

            judgments.append({
                'diary_number': diary_number,
                'case_number': case_number,
                'by': judgment_by,
                'bench': bench,
                'petitioner': {
                    'name': petitioner_name,
                    'advocate': petitioner_advocate,
                },
                'respondent': {
                    'name': respondent_name,
                    'advocate': respondent_advocate
                },
                'document_href': __BASE_URL__ + judgment_url
            })

    if debug:
        print()
    return judgments

def main():
    parser = argparse.ArgumentParser(
        description="Retrieve judgment information for judgments between a given date range."
    )
    parser.add_argument(
        '-d', '--debug', action='store_true', help='enable debugging output'
    )
    parser.add_argument(
        'start_date', type=datetime.date, help='load judgments from this date',
        default=datetime.datetime.now().date() - datetime.timedelta(days=28), nargs='?'
    )
    parser.add_argument(
        'end_date', type=datetime.date, help='load judgments upto this date',
        default=datetime.datetime.now().date(), nargs='?'
    )

    try:
        args = parser.parse_args()
        judgments = get_judgements_by_date(
            args.start_date, args.end_date, debug=args.debug
        )
        content = {
            'meta': {
                'start_date': args.start_date.isoformat(),
                'end_date': args.end_date.isoformat()
            },
            'data': judgments
        }
        print(json.dumps(content, ensure_ascii=False, indent=4), sep='\n')
    except requests.RequestException:
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()