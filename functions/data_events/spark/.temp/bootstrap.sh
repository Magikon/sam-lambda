#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
sudo pip-3.6 install -U pandas numpy boto3 pytz python-dateutil botocore s3transfer pydash pycrypto

# install psycopg2
sudo yum install -y gcc postgresql-devel
sudo curl https://bootstrap.pypa.io/ez_setup.py -o - | sudo python36
sudo pip-3.6 install -U psycopg2