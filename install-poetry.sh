#!/bin/bash

#
# Install and setup poetry:
# https://python-poetry.org/docs/#installing-with-the-official-installer
#

curl -sSL https://install.python-poetry.org | python3 -
poetry --version
poetry completions bash >> ~/.bash_completion
poetry config virtualenvs.in-project true
poetry init
poetry install --no-root
