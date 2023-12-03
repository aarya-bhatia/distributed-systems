import os

os.system("go run . 1 rmdir /")

map_exe = "maplejuice_exe/join_mapper.py"
reduce_exe = "maplejuice_exe/join_reducer.py"

os.system(f"go run . 1 put {map_exe} {map_exe}")
os.system(f"go run . 1 put {reduce_exe} {reduce_exe}")

for i in [1000]:
    file1 = f"data/customers_{i}.csv"
    file2 = f"data/orders_{i}.csv"

    if not os.path.exists(file1):
        print("file not found:", file1)
        continue

    if not os.path.exists(file2):
        print("file not found:", file2)
        continue

    os.system(f"go run . 1 put {file1} {i}/{file1}")
    os.system(f"go run . 1 put {file2} {i}/{file2}")

    map_args = f"{i}/{file1} 2 {i}/{file2} 3"
    reduce_args = f"{i}/{file1} {i}/{file2}"

    os.system(f"go run . 1 maple {map_exe} 4 prefix_{i} {i}/data {map_args}")
    os.system(f"go run . 1 juice {reduce_exe} 4 prefix_{i} output_{i} {reduce_args}")
