#!/bin/bash

# create Admin user, you can read these values from env or anywhere else possible
superset fab create-admin --username admin --firstname abc --lastname xyz --email email@com --password password

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 

# Starting server
/bin/sh -c /usr/bin/run-server.sh