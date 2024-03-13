'''
Reads zip compressed USENET mailboxes and save messages to csv files named <usenet_group_name>_messages.csv
with the following fields: ID, SUBJECT, MESSAGE_CONTENT, DATE
'''

import os
import mailbox
import dateutil.parser
import zipfile
import csv
import argparse
import multiprocessing as mp

from util_msg import clean_message_id, get_message_text, clean_subject_text, clean_message_text
from util import create_ifnotexists_directory


def get_messages_from_mbox(mbox):
    messages = []

    # read messages
    num_messages = 0
    for msg_id in mbox.keys():
        try:
            msg = mbox[msg_id]
            msg_item = {}
            msg_item['id'] = clean_message_id(msg['Message-ID'])
            if msg_item['id'] is None:
                continue

            msg_item['subject'] = clean_subject_text(msg['Subject'])
            body_text = get_message_text(msg)
            if body_text is None:
                continue
            msg_item['text'] = clean_message_text(body_text)

            date = str(msg['Date'])
            try:
                dt = dateutil.parser.parse(date, fuzzy=True)
            except Exception as e:
                print("  Exception: ", e)
                continue
            msg_item['date'] = f'{dt.year}/{dt.month}/{dt.day}'
            messages.append(msg_item)
            num_messages += 1
        except:
            continue
    print(f' number of processed messages: {num_messages}')
    return messages


def write_message_csv(output_folder, file_name, mbox):
    print(f' mbox: {file_name}')
    messages = get_messages_from_mbox(mbox)

    # write csv
    output_path = os.path.join(output_folder, file_name + '_messages.csv')
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
        for msg_item in messages:
            writer.writerow((msg_item['id'], msg_item['date'], msg_item['subject'], msg_item['text']))


def get_mbox_list(input_folder):
    mbox_file_list = []

    files_zip = [file for file in os.listdir(input_folder) if file.endswith('.zip')]
    for file in files_zip:
        zip_file_path = os.path.join(input_folder, file)
        mbox_file_list.append(zip_file_path)

    return mbox_file_list


def process_mbox(zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        for file_name in zip_file.namelist():
            with zip_file.open(file_name) as file_mbox:
                print(f'Processing {file_name}')
                content_mbox = file_mbox.read()
                with open(file_name, 'wb') as f:
                    f.write(content_mbox)
                mbox = mailbox.mbox(file_name, factory=None, create=False)
                os.remove(file_name)

                write_message_csv(output_folder, file_name, mbox)
                print()


def init_worker(inputfolder, outputfolder):
    global input_folder, output_folder
    output_folder = outputfolder
    input_folder = inputfolder


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_folder', type=str, required=True)
    parser.add_argument('--output_folder', type=str, required=True)
    args = parser.parse_args()

    global output_folder, input_folder

    input_folder = args.input_folder
    output_folder = args.output_folder
    create_ifnotexists_directory(output_folder)

    mbox_file_list = get_mbox_list(input_folder)

    num_process = mp.cpu_count() - 1
    print(f'Processing USENET files (num_process: {num_process}) ...')

    with mp.Pool(num_process, initializer=init_worker, initargs=(input_folder, output_folder,)) as p:
        p.map(process_mbox, mbox_file_list)


if __name__ == "__main__":
    main()




