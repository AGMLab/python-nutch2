"""
Settings for the app
"""
try:
    from local_settings import NUTCH_BIN_DIR
except ImportError:
    NUTCH_BIN_DIR = '/home/emre/git/nutch-2.1/runtime/local/bin'
