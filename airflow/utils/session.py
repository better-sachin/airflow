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

import contextlib
from functools import wraps
from typing import Callable, TypeVar

from sqlalchemy.exc import OperationalError

from airflow import settings


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
    session = settings.Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


RT = TypeVar("RT")  # pylint: disable=invalid-name


def provide_session(func: Callable[..., RT], attempts=2, exceptions=(OperationalError,)) -> Callable[..., RT]:
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        arg_session = 'session'

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        def retryable_transaction(session, attempts=2, exceptions=(OperationalError,)):
            for i in range(attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if i == (attempts - 1):
                        raise
                    session.rollback()

        if session_in_kwargs:
            return retryable_transaction(kwargs[arg_session], attempts, exceptions)
        elif session_in_args:
            return retryable_transaction(func_params[func_params.index(arg_session)], attempts, exceptions)
        else:
            with create_session() as session:
                kwargs[arg_session] = session
                return retryable_transaction(session, attempts, exceptions)

    return wrapper

#
# @contextlib.contextmanager
# def retryable_transaction(session, attempts=3, exceptions=(OperationalError,)):
#     try:
#         with tenacity.Retrying(
#             retry=tenacity.retry_if_exception_type(exception_types=exceptions),
#             wait=tenacity.wait_random_exponential(multiplier=0.2, max=3),
#             stop=tenacity.stop_after_attempt(attempts)
#         ):
#             yield session
#     except exceptions:
#         session.rollback()
#         raise
#
#     # https://github.com/nameko/nameko-sqlalchemy/blob/master/nameko_sqlalchemy/transaction_retry.py
#     # https://github.com/bslatkin/dpxdt/blob/2e582a9285c3b6abdc00dbb3f0430b6d7e4a647c/dpxdt/server/utils.py#L37-L58
#
