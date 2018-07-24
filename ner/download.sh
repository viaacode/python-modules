#!/usr/bin/env bash

if [ "$#" -ne 2 ]; then
    echo "2 arguments expected" >&2
    echo "Usage: $0 file alias"
    exit 1
fi

cd `dirname $0`
if [ -d "$2" ]; then
	echo "'$d' directory already exists"
	exit 1
fi

zip="${1##*/}"
dir="${zip%.zip}"

wget -c "$1"
unzip "$zip"
rm "$zip"

mv "$dir" "$2"
