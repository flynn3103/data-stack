import ssl
import urllib3

# Disable SSL warnings (use only in a testing environment)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context
