#!/bin/bash
mkdir -p logs
mkdir -p /tmp

touch logs/odbx_dcgat_access.log
touch logs/odbx_dcgat_error.log

tail -f -n 20 logs/odbx_dcgat_access.log logs/odbx_dcgat_error.log &

python /app/dcgat_optimade/utils.py &

PYTHONPATH="/app/optimade_dcgat"

gunicorn \
    -w 1 \
    -k uvicorn.workers.UvicornWorker \
    --timeout 500 \
    --error-logfile logs/odbx_dcgat_error.log \
    --access-logfile logs/odbx_dcgat_access.log \
    --capture-output \
    --access-logformat "%(t)s: %(h)s %(l)s %(u)s %(r)s %(s)s %(b)s %(f)s %(a)s" \
    -b unix:/tmp/gunicorn_rest_dcgat.sock dcgat_optimade.app:app
