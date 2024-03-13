import re


MIN_TEXT_LEN = 30
REPLY_BEGINNING = re.compile(r'^[>]+')
ADMITED_CHARSETS = ['iso-8859-1', 'utf-8', 'ascii', 'iso-8859-15', 'windows-1252', 'us-ascii', 'iso-8859-9',
                    'iso-8859-4', 'windows-1256', 'iso-8859-14', 'iso-8859-13', 'iso-5589-1', 'iso-8895-1']
