#!/bin/bash

#!/bin/bash

pause() {
    local dummy
    read -rsn 1 -p "Press any key to continue..."
}

# map <output> <input>
# juice <input> <output>

echo "sending map job 1 (input=traffic, output=prefix1)"
pause
go run . maple demo_map1 1 prefix1 traffic Fiber
echo "sending reduce job 1 (input=prefix1, output=output1)"
pause
go run . juice demo_reduce1 1 prefix1 output1
echo "sending map job 2 (input=output1, output=prefix2)"
pause
go run . maple demo_map2 1 prefix2 output1
echo "sending reduce job 2 (input=prefix2, output=output2)"
pause
go run . juice demo_reduce2 1 prefix2 output2

echo "done."
