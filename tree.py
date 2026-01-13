import os

def print_tree(dir_path, prefix=""):
    contents = sorted(os.listdir(dir_path))
    for i, name in enumerate(contents):
        # Skip .git folder
        if name == ".git":
            continue

        path = os.path.join(dir_path, name)
        connector = "└── " if i == len(contents)-1 else "├── "
        print(prefix + connector + name)
        if os.path.isdir(path):
            extension = "    " if i == len(contents)-1 else "│   "
            print_tree(path, prefix + extension)

print_tree(".")
