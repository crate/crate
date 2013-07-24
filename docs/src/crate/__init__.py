# this is a namespace package
try:
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
    pass
