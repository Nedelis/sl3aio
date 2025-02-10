from pathlib import Path
from sys import path

path.insert(0, (Path(__file__).parents[2].resolve() / 'src').as_posix())

project = 'sl3aio'
copyright = '2024-2025, Nedelis'
author = 'Nedelis'
release = '1.3.0-rc1'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_copybutton'
]

autodoc_member_order = 'bysource'

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'shibuya'
html_static_path = ['_static']
