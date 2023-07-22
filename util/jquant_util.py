# a decorator receives the method it's wrapping as a variable 'f'
import jqdatasdk as jq

"""聚宽认证装饰器函数"""


def auth(username="", password=""):
    jq.auth(username, password)


def jquant_auth(f, username="", password=""):
    # we use arbitrary args and keywords to
    # ensure we grab all the input arguments.
    def wrapped_f(*args, **kw):
        # note we call f against the variables passed into the wrapper,
        # and cast the result to an int and increment .
        jq.auth(username, password)
        return f(*args, **kw)

    return wrapped_f  # the wrapped function gets returned.
