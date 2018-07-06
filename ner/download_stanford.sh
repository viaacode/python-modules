#!/bin/bash

cd `dirname $0`
if [ -d stanford ]; then
	echo "stanford directory already exists"
	exit 1
fi
cd stanford

# ner https://nlp.stanford.edu/software/CRF-NER.shtml
wget -c https://nlp.stanford.edu/software/stanford-ner-2018-02-27.zip

unzip stanford-ner-2018-02-27.zip
rm stanford-ner-2018-02-27.zip
mv stanford-ner-2018-02-27 stanford
