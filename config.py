#!/usr/bin/env python
# coding:utf-8

import os
base_dir = os.path.dirname (__file__)
# for log.py
log_dir = os.path.join (base_dir, "log")
log_rotate_size = 20000
log_backup_count = 3
log_level = "INFO"
# for log.py

SSL_CERT = os.path.join(base_dir, 'private/server.pem')
RUN_DIR = os.path.join (base_dir, "run")

SERVERS = {
    1:  ("127.0.0.1", 12201),
    2:  ("127.0.0.1", 12202),
    3:  ("127.0.0.1", 12203),
}

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :
