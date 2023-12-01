# -----------------------
grep -c "Video,Radio" data/traffic.csv
13
grep -c "Video\|Radio" data/traffic.csv
67
grep -c "Video.*Radio" data/traffic.csv
16
# -----------------------

grep -c "Video,Radio" output
13

grep -c "Video\|Radio" output
67

grep -c "Video.*Radio" output
16

# ------------------------
./cleanup.sh 2>/dev/null
./cat.sh output >/dev/null 2>&1
# ------------------------

go run . 1 lsdir / 2>/dev/null
