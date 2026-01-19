# TODO: there are duplicate IDs in the data??

import pathlib
import json
import jsondiff
data = json.load(pathlib.Path("all_products_all_data.json").open("r", encoding="utf-8"))
print(f"Total products loaded: {len(data)}")
colliding_ids=0
ids_seen={}
for product in data:
    pid = product.get("id")
    if pid in ids_seen:
        colliding_ids += 1
        print(f"Colliding ID found: {pid}")
#        print(json.dumps(product, ensure_ascii=False, indent='\t'))
#        print(ids_seen[pid])
        # json diff
        diff = jsondiff.diff(ids_seen[pid], product)
        if diff:
            print(json.dumps(diff, ensure_ascii=False, indent='\t'))
            print("-----")
        

    else:
        ids_seen[pid]=product
print(f"Total colliding IDs: {colliding_ids}")