import json
import glob

file_types = ['base', 'alert', 'average']

for type_file in file_types:
    result = []
    for f in glob.glob(f"C:/Users/Stefanche/Documents/GitHub/Inzinjerstvo/spark-weather-processing/bigquery/{type_file}/*.json"):
        try:
            with open(f, "r", encoding="utf-8") as infile:
                content = infile.read()
                if not content.strip():
                    print(f"Skipping empty file: {f}")
                    continue
                
                try:
                    file_as_json = json.loads(content)
                    result.append(file_as_json)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file: {f}")
                    print(f"Content: {content}")
                    print(f"Error: {e}")
        except Exception as e:
            print(f"Error reading file: {f}")
            print(f"Error: {e}")

    with open(f"merged_{type_file}_file.json", "w", encoding="utf-8") as outfile:
        json.dump(result, outfile, ensure_ascii=False, indent=4)
