# encoding=utf-8

import os
import sys

__all__ = [i.replace(".py", "") for i in os.listdir(os.path.dirname(__file__))]
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.dirname(__file__).join(os.listdir(os.path.dirname(__file__))))
