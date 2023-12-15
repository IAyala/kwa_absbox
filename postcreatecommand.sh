#!/bin/bash

python3 -m pip install --upgrade pip
pip3 install poetry==1.3.2
poetry config virtualenvs.create false
poetry update