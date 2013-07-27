#!/bin/bash

source /home1/02554/sagatut/sagaenv/bin/activate
pip install PIL

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

python $DIR/mandelbrot.py $@