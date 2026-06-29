# https://huggingface.co/datasets/Abirate/english_quotes
import json

quote_list = []

with open("quotes.jsonl", "r") as f:
    for line in f:
        quote = json.loads(line)["quote"]
        quote_list.append(quote)
