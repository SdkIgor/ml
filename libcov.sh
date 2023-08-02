#!/usr/bin/env bash
export PYTHONPATH="$PWD/lib"
coverage run --source=lib -m unittest && coverage report
