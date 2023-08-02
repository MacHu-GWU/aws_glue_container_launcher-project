# -*- coding: utf-8 -*-

import boto3

boto_ses = boto3.session.Session()
cred = boto_ses.get_credentials()
print(cred)
print(cred.access_key)
print(cred.secret_key)
print(cred.token)