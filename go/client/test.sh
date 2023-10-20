#!/bin/sh

rm -rf *.out

go build .

echo "Uploading files..."

./client put data/small small &
./client put data/mid small &
./client put data/large small &

echo "Downloading files..."

./client get small small &
./client get mid mid &
./client get large large &

sleep 60

echo "Comparing files..."

if [ -f small ]; then
	diff small small || echo "small file is different"
else
	echo "Failed to download small"
fi

if [ -f mid ]; then
	diff mid mid || echo "mid file is different"
else
	echo "Failed to download mid"
fi

if [ -f large ]; then
	diff large large || echo "large file is different"
else
	echo "Failed to download large"
fi

echo "Cleaning up..."

rm -rf small mid large

