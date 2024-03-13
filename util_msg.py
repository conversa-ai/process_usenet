import hashlib
import dateutil.parser
import chardet
import pycld2 as cld2
import config
import re
import os
import sys
import csv

from util import multiple_replace
from email.header import decode_header



replacements = {"xFrom": "From", "xxFrom": "From"}


def hash_mail_address(mail_address):
    bytes_mail_address = mail_address.encode('utf-8')
    hash_object = hashlib.sha256(bytes_mail_address)
    hash_hex = hash_object.hexdigest()
    return hash_hex


def save_messages_mbox(box, box_name, output_folder):
    filenum = 1

    for msg_id in box.keys():
        try:
            msg = box[msg_id]
            date = str(msg['Date'])
            mid = str(msg["Message-ID"]).replace("#1/1", "").strip()
            try:
                dt = dateutil.parser.parse(date, fuzzy=True)
            except Exception as e:
                print("  Exception: ", e)
                continue

            dir_path = f'{output_folder}/{box_name}/{dt.year}/{dt.month}/{dt.day}'
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)

            file_path = f"{dir_path}/{mid}.txt"
            with open(file_path, "w") as f:
                txt = msg.as_string()
                content = multiple_replace(replacements, txt)
                f.write(content)
                filenum += 1
        except Exception as e:
            print("  Exception: ", e)
            continue

    return filenum


def create_dialog_msg_hash(msg_id_list):
    hash_msg_list = []
    output_dialog_hash = ''
    for msg_id in msg_id_list:
        hash_msg_id = clean_message_id(msg_id)
        hash_msg_list.append(hash_msg_id)
        output_dialog_hash += hash_msg_id
    return output_dialog_hash, hash_msg_list


def clean_message_id(msg_id):
    return_str = str(msg_id).replace("#1/1", "").strip().lower()
    if return_str == 'none':
        return None
    return hash_mail_address(return_str)


def get_message_text(message):  # getting plain text 'email body'
    body = None
    if message.is_multipart():
        for part in message.walk():
            if part.is_multipart():
                for subpart in part.walk():
                    if subpart.get_content_type() == 'text/plain':
                        body = subpart.get_payload(decode=True)
            elif part.get_content_type() == 'text/plain':
                body = part.get_payload(decode=True) # True
    elif message.get_content_type() == 'text/plain':
        body = message.get_payload(decode=True) # True


    # TODO problema codificacion
    if body is not None:
        try:
            charset = message.get_charsets()
            if charset[0] is not None and 'None' not in charset[0] and len(charset) > 0:
                # if charset[0].lower() not in admited_charsets:
                #     print('Codificación no admitida: ' + charset[0])
                #     return None
                body = body.decode(charset[0])  # normalmente iso-8859-1
            else:
                charset_detected = chardet.detect(body)
                if charset_detected['confidence'] > 0.7:
                    detected_encoding = charset_detected['encoding'].lower()
                    # if detected_encoding not in admited_charsets:
                    #     print('Codificación no admitida detectada: ' + detected_encoding)
                    #     return None
                    try:
                        body = body.decode(detected_encoding)
                    except UnicodeDecodeError:
                        print('  Decoding error: ' + str(detected_encoding))
                        return None
                else:
                    return None

        except Exception as ex:
            print('  Exception during decoding: ' + str(ex))
            return None

    # remove non spanish text
    detected_language = cld2.detect(body, returnVectors=True)
    isReliable, textBytesFound, details, vectors = cld2.detect(body, returnVectors=True)
    if not isReliable:
        return None
    body_es = ''
    for vector in vectors:
        if vector[3] == 'es':
            body_fragment = body[vector[0]:vector[1]]
            body_es += body_fragment + ' '

    body_es = body_es.strip()
    if len(body_es) < config.MIN_TEXT_LEN:
        return None
    return body_es


def clean_message_text(message_content):
    cleaned_content = ''

    rows = message_content.split('\n')
    for row in rows:
        row = row.strip()
        # remove from: b'On Wed, 17 Oct 2007 21:34:17 +0200, "Ariel" <font@ya.com> wrote:\n\n>\n>
        if 'wrote:\n' in row.lower():
            continue
        if row.lower().strip().startswith('from:') or row.lower().strip().startswith('to:')\
                or row.lower().strip().startswith('subject:') or row.lower().strip().startswith('de:'):
            continue

        # remove replies \n>>
        if re.match(config.REPLY_BEGINNING, row):
            continue

        # lineas vacias
        if len(row.strip()) == 0:
            continue

        # TODO remove greetings and goodbyes

        cleaned_content += row + ' '

    return cleaned_content.strip().replace('\n', ' ').replace('\r', ' ')


def clean_subject_text(original_text):
    decoded_titulo = decode_header(original_text)

    decoded_titulo_str = ''
    for text, encoding in decoded_titulo:
        decoded_titulo_str += text.decode(encoding) if encoding else text

    return decoded_titulo_str.replace('\n', ' ').replace('\r', ' ')

def new_dialog_contains_existing_dialog(hash, active_hash_list):
    for active_hash in active_hash_list:
        if hash.startswith(active_hash):
            return active_hash
    return None


def new_dialog_is_contained_on_existing_dialog(hash, active_hash_list):
    for active_hash in active_hash_list:
        if active_hash.startswith(hash):
            return True
    return False


def get_dialog_tuple(dialog_hash, dialog_chain_hash_list, mbox_file_name, messages):
    dialog = {}
    dialog['dialog_hash'] = dialog_hash
    dialog['dialog_hash_chain'] = dialog_chain_hash_list

    # message contents
    dialog_contents = []
    mbox_messages = messages[mbox_file_name]
    for msg_hash in dialog_chain_hash_list:
        msg_result = [msg for msg in mbox_messages if msg['id'] == msg_hash]
        if len(msg_result) == 0:
            return None
        dialog_contents.append(msg_result[0]['text'])
    dialog['dialog_txt_chain'] = dialog_contents

    return dialog


def read_messages_csv(input_msg_csv_folder):
    messages = {}

    files_csv = [file for file in os.listdir(input_msg_csv_folder) if file.endswith('.csv')]

    if len(files_csv) == 0:
        print(f'mbox csv files not found: {input_msg_csv_folder}')
        sys.exit(1)

    field_names = ['id', 'date', 'subject', 'text']

    for file_name in files_csv:
        message_data = []
        csv_file_path = os.path.join(input_msg_csv_folder, file_name)
        with open(csv_file_path, mode="r") as file:
            csv_reader = csv.DictReader(file, delimiter=";", fieldnames=field_names)
            for row in csv_reader:
                message_data.append(row)

        mbox_name = file_name.rstrip('_messages.csv')
        messages[mbox_name] = message_data

    return messages