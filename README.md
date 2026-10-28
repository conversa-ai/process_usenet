# Parsing UseNET dialogues from the newsgroup mboxs

```
python generate_messages_csv.py --input_folder input_test --output_folder output_test
python generate_dialogs.py --input_folder input_test --output_folder output_test --input_msg_csv_folder output_test
```

## Version 1.3 rehydration

See [README-rehydration.md](README-rehydration.md) for the bounded standalone tool, exact-match checks, dependency requirements and synthetic tests. This addition does not relicense the earlier extraction code.
