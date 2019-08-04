# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 23:36:07 2019

@author: ydima
"""

import pandas as pd
import pytest

# setup
df = pd.DataFrame({'col1': ['Sam, and', 'Frodo', 'Merry'],
                   'col2': ['the ring', 'Morder', 'Smeagol'],
                   'col3': ['"The Lord of the Rings"', 'Gandalf', 'Bilbo'],
                   'col4': [x for x in range(2107,2110)] })




