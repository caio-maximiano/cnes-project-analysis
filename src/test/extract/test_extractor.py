import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import os
import zipfile
from unittest import mock

import pytest

from main.extract.extractor import Extractor
