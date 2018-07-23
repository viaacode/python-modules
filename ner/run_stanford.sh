#!/bin/bash

if [ "$1" == "--help" ]; then
	echo "Usage: $0 file"
	echo "	file Full path filename of text file to tag"
	exit 0
fi


p="$(cd "$(dirname "$0")"; pwd)"

if [ ! -d "$p/stanford" ]; then
	echo "Missing directory 'stanford', did you run download_stanford.sh already?" >&2
	exit 1
fi

scriptdir="$p/stanford"
cd "$scriptdir"

CLASSIFIER=${CLASSIFIER:-english.all.3class.distsim.crf.ser.gz}
INPUT_ENCODING=${INPUT_ENCODING:-UTF-8}
OUTPUT_ENCODING=${OUTPUT_ENCODING:-UTF-8}
TOKENIZER_FACTORY=${TOKENIZER_FACTORY:-edu.stanford.nlp.process.WhitespaceTokenizer}

cmd="java -mx700m -cp $scriptdir/stanford-ner.jar:$scriptdir/lib/\*"
args=" -loadClassifier $scriptdir/classifiers/$CLASSIFIER  -inputEncoding $INPUT_ENCODING -outputEncoding $OUTPUT_ENCODING -tokenizerOptions untokenizable=noneDelete "
args="$args -tokenizerFactory $TOKENIZER_FACTORY -encoding utf8 "
if [ "$#" -lt 1 ]; then
	${cmd} edu.stanford.nlp.ie.NERServer ${args} -port 9001
else
	${cmd} edu.stanford.nlp.ie.crf.CRFClassifier ${args} -textFile "$1"
fi
