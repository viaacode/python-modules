#!/bin/bash

if [ "$1" == "--help" ]; then
	echo "Usage: $0 file"
	echo "	file Full path filename of text file to tag"
	exit 0
fi


p="$(cd "$(dirname "$1")"; pwd)"

if [ ! -d "$p/stanford" ]; then
	echo "Missing directory 'stanford', did you run download_stanford.sh already?" >&2
	exit 1
fi

scriptdir="$p/stanford"
cd "$scriptdir"

args=" -mx700m -cp $scriptdir/stanford-ner.jar:$scriptdir/lib/\*"
args2=" -loadClassifier $scriptdir/classifiers/english.all.3class.distsim.crf.ser.gz  -inputEncoding UTF-8 -outputEncoding UTF-8"

if [ "$#" -lt 1 ]; then
	java ${args} edu.stanford.nlp.ie.NERServer ${args2} -port 9001
else
	java ${args} edu.stanford.nlp.ie.crf.CRFClassifier ${args2} -textFile "$1"
fi
