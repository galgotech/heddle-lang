#!/bin/bash

PYTHONPATH=$PYTHONPATH:src poetry run python -m unittest $@
