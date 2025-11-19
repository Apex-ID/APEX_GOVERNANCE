# Configuration file for the Sphinx documentation builder.
import os
import sys
import django

# --- Ajuste do caminho do projeto -----------------------------------------
# Caminho até a raiz do projeto Django
# (docs/source → docs → raiz do projeto)
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('../..'))
sys.path.insert(0, os.path.abspath('../../APEX_GOVERNANCE'))
# --- Configuração do Django ------------------------------------------------
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'apex_project.settings')
django.setup()
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'APEX_GOVERNANCE'
copyright = '2025, Sergio Santana dos Santos'
author = 'Sergio Santana dos Santos'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',      # gera documentação automaticamente a partir do código
    'sphinx.ext.napoleon',     # suporta docstrings Google / NumPy
    'sphinx.ext.viewcode',     # adiciona links para o código-fonte
    'sphinx.ext.todo',         # Suporte para diretivas TODO
    'sphinxcontrib.plantuml',
]

plantuml = 'plantuml'
plantuml_output_format = 'svg' # ou 'png'

templates_path = ['_templates']
exclude_patterns = []
language = 'pt-BR'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
autoclass_content = 'both'
