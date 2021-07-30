import json
import pandas as pd
from pyvi import ViTokenizer

if __name__ == '__main__':
    fRead = open("/content/data_3_sonvt35.json", "r", encoding="utf8")
    fWrite = open("/content/result/data_3_sonvt35.json", "a", encoding="utf8")
    Lines = fRead.readlines()
    count = 0
    cleanMap = lambda x: x.replace("_", " ")
    for line in Lines:
        count += 1
        if count % 2 != 0:
            fWrite.write(line)
        else:
            json_object = json.loads(line.strip())
            data = ViTokenizer.tokenize(json_object["title"]).split()
            data = filter(lambda x: len(x) > 1, data)
            data = list(map(cleanMap, data))
            fWrite.write("{" + "\"suggest_title\": [\"" + '", "'.join(data) + "\"]}\n")
    fRead.close()
    fWrite.close()