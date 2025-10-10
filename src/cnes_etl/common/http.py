from __future__ import annotations
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Verificação TLS: usa Keychain (macOS) se disponível, senão certifi ---
try:
    import truststore  # type: ignore
    truststore.inject_into_ssl()
    _VERIFY = True  # com truststore, verify=True já usa o sistema
except Exception:
    import certifi  # type: ignore
    _VERIFY = certifi.where()

_DEF_STATUS = (429, 500, 502, 503, 504)

def new_session(total: int = 5, backoff: float = 2.0) -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=total, backoff_factor=backoff, status_forcelist=_DEF_STATUS)
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.verify = _VERIFY  # <- chave!
    return sess