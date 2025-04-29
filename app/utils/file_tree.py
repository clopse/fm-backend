import os

def build_file_tree(base_path: str, public_url_base: str):
    tree = []

    if not os.path.exists(base_path):
        return tree

    for item in sorted(os.listdir(base_path)):
        full_path = os.path.join(base_path, item)

        if os.path.isdir(full_path):
            tree.append({
                "name": item,
                "children": build_file_tree(full_path, public_url_base)
            })
        else:
            file_url = f"{public_url_base}/{full_path.split('storage/')[-1].replace(os.sep, '/')}"
            tree.append({
                "name": item,
                "url": file_url
            })

    return tree
