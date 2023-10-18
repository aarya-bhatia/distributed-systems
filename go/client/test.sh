#!/bin/sh

go build .

echo "Uploading files..."

./client put data/small small &
./client put data/mid small &
./client put data/large small &

echo "Downloading files..."

./client get small small &
./client get mid mid &
./client get large large &

sleep 5

echo "Comparing files..."

[ -f small ] && diff small small || echo "small file is different"
[ -f mid ] && diff mid mid || echo "mid file is different"
[ -f large ] && diff large large || echo "large file is different"

echo "Cleaning up..."

rm -rf small mid large

