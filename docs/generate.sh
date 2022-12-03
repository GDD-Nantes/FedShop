#!/bin/bash

if [ -z "Makefile" ]; then
    echo "Empty docs folder, generatting new project..." &&
    sphinx-quickstart -q -p "RSFB" -a "Minh-Hoang DANG" -v "0.0.1"​
fi

echo "Generating docs..."
sphinx-apidoc -f -o . ../​utils

