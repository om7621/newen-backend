#!/bin/bash
pip install -r requirements.txt
gunicorn app:app