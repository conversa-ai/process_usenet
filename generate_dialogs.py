'''
Reads zip compressed USENET mailboxes and save messages reply chains as dialogs in a json file
named <usenet_group_name>_dialog_ids.json
with the structure:
{
   {ID_dialog, ID_root, ID_first_child, ID_second_child, ...},                   (dialog chain 1)
   {ID_dialog, ID_root, ID_first_child, ID_second_child, ...},                   (dialog chain 2)
   ....
}
'''

import os
import mailbox
import zipfile
import json
import csv
import argparse
import multiprocessing as mp
import sys

from util_msg import create_dialog_msg_hash, new_dialog_contains_existing_dialog, \
    new_dialog_is_contained_on_existing_dialog, get_dialog_tuple, read_messages_csv
from util import create_ifnotexists_directory


def get_dialog_chains(mbox, mbox_file_name):
    dialogs_chains = []
    active_dialog_hashes = []

    # read dialogs
    num_dialogs = 0
    for msg_id in mbox.keys():
        try:
            msg = mbox[msg_id]
            msg_references = msg['References']
            if msg_references is None:
                continue
            # print('msg_references: ' + str(msg_references))
            dialog_msg_ids = msg_references.split(' ')
            dialog_hash, dialog_chain_hash_list = create_dialog_msg_hash(dialog_msg_ids)

            # test min dialog length
            if len(dialog_msg_ids) < 2:
                continue

            # test if the new dialog chain contains existing dialog chains
            active_dialog_hash = new_dialog_contains_existing_dialog(dialog_hash, active_dialog_hashes)
            if active_dialog_hash is not None:

                # remove dialogs_chains contained on previous dialogs
                dialog = get_dialog_tuple(dialog_hash, dialog_chain_hash_list, mbox_file_name, messages)
                if dialog is not None:
                    active_dialog_hashes.remove(active_dialog_hash)
                    active_dialog_hashes.append(dialog_hash)

                    dialogs_chains = [dialog for dialog in dialogs_chains if dialog['dialog_hash'] != active_dialog_hash]
                    dialogs_chains.append(dialog)
                    num_dialogs += 1

            else:
                # test if the new dialog chain is contained in a existing dialog chain
                #  because messages could be unordered
                if new_dialog_is_contained_on_existing_dialog(dialog_hash, active_dialog_hashes):
                    continue
                else:
                    dialog = get_dialog_tuple(dialog_hash, dialog_chain_hash_list, mbox_file_name, messages)
                    if dialog is not None:
                        active_dialog_hashes.append(dialog_hash)
                        dialogs_chains.append(dialog)
                        num_dialogs += 1
        except Exception as e:
            print("Exception: ", e)
            continue
    print(f' number of dialogs: {num_dialogs}/{len(mbox.keys())}')
    return dialogs_chains


def write_dialog_chain_json(output_folder, file_name, mbox):
    print(f' mbox: {file_name}')
    dialog_chains = get_dialog_chains(mbox, file_name)

    # write json
    output_path = os.path.join(output_folder, file_name + '_dialog_ids.json')
    with open(output_path, "w") as json_file:
        json.dump(dialog_chains, json_file)


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

                write_dialog_chain_json(output_folder, file_name, mbox)
                print()


def init_worker(inputfolder, outputfolder, messages_data):
    global input_folder, output_folder, messages
    output_folder = outputfolder
    input_folder = inputfolder
    messages = messages_data



def main():
    # --input_folder=usenet-es --input_msg_csv_folder=output_usenet-es --output_folder=output_usenet-es_dialogs
    # --input_folder=input_test --output_folder=output
    # --input_folder=input_test_simple --input_msg_csv_folder=output --output_folder=output
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_folder', type=str, required=True)
    parser.add_argument('--output_folder', type=str, required=True)
    parser.add_argument('--input_msg_csv_folder', type=str, required=True)
    args = parser.parse_args()

    global output_folder, input_folder, messages

    input_folder = args.input_folder
    output_folder = args.output_folder
    input_msg_csv_folder = args.input_msg_csv_folder
    create_ifnotexists_directory(output_folder)

    messages = read_messages_csv(input_msg_csv_folder)

    mbox_file_list = get_mbox_list(input_folder)

    #num_process = 1
    num_process = mp.cpu_count() - 1
    print(f'Processing USENET files (num_process: {num_process}) ...')

    with mp.Pool(num_process, initializer=init_worker, initargs=(input_folder, output_folder, messages)) as p:
        p.map(process_mbox, mbox_file_list)


if __name__ == "__main__":
    main()