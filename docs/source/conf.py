from pathlib import Path
from sys import path

path.insert(0, (Path(__file__).parents[2].resolve() / 'src').as_posix())

project = 'sl3aio'
copyright = 'Copyright © 2024-2025, Nedelis'
author = 'Nedelis'
release = '1.3.0-rc1'

github_url = 'https://github.com/Nedelis/sl3aio'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.extlinks',
    'sphinx_copybutton',
    'sphinx_togglebutton',
    'sphinx_design'
]

extlinks = {
    'github-page': (f'{github_url}%s', None)
}

autodoc_member_order = 'bysource'

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'shibuya'
html_static_path = ['_static']
html_theme_options = {
  'accent_color': 'red',
  'github_url': github_url
}
html_context = {
  'source_type': 'github',
  'source_user': 'Nedelis',
  'source_repo': 'sl3aio',
  'source_docs_path': '/docs/source/'
}
