# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Default authentication backend - everything is allowed"""
from functools import wraps
from flask_login import current_user
CLIENT_AUTH = None


def init_app(_):
    """Initializes authentication backend"""


def requires_authentication(function):
    """Decorator for functions that require authentication"""
    
    @wraps(function)
    def decorated(*args, **kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin()
        logger.log.error(f"Current user {current_user.__dict__}")
        logger.log.error(f"Roles {current_user.roles}")
        return function(*args, **kwargs)

    return decorated
