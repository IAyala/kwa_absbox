#!/bin/bash

python3 -m pip install --upgrade pip
pip3 install poetry
poetry config virtualenvs.create false
poetry update