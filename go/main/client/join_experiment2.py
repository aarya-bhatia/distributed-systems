import os

os.system("go run . 1 rmdir /")

exe_dir = f"../../maplejuice/maplejuice_exe"
os.system(f"go run . 1 put {exe_dir}/join_mapper.py join_mapper.py")
os.system(f"go run . 1 put {exe_dir}/join_reducer.py join_reducer.py")

map_exe = "join_mapper.py"
reduce_exe = "join_reducer.py"

# for i in [20, 100, 200, 500]:
for i in [500]:
    file1 = f"data/customers_{i}.csv"
    file2 = f"data/orders_{i}.csv"

    if not os.path.exists(file1) or not os.path.exists(file2):
        continue

    os.system(f"go run . 1 put {file1} {i}/{file1}")
    os.system(f"go run . 1 put {file2} {i}/{file2}")

    map_args = f"{i}/{file1} 2 {i}/{file2} 3"
    reduce_args = f"{i}/{file1} {i}/{file2}"

    os.system(f"go run . 1 maple {map_exe} 4 prefix_{i} {i}/data {map_args}")
    os.system( f"go run . 1 juice {reduce_exe} 4 prefix_{i} output_{i} {reduce_args}")
