"""Rewrite relative links for the docs site.

1. ../../scripts/ and ../../src/ → GitHub blob URLs
2. docs/<path> → <path> (strips docs/ prefix so README.md links work
   when symlinked as docs/index.md)
"""

from __future__ import annotations

import re

from markdown import Extension
from markdown.preprocessors import Preprocessor

_REPO = 'https://github.com/globus-labs/icicle/blob/v4-dev'
_RE_GITHUB = re.compile(
    r'\[([^\]]+)\]\((?:\.\./)+((?:scripts|src)/[^)]+)\)',
)
_RE_DOCS = re.compile(r'\[([^\]]+)\]\(docs/((?:icicle|ingest)/[^)]+)\)')
# Strip docs/ from HTML src attributes (e.g. <img src="docs/...">)
_RE_DOCS_SRC = re.compile(r'src="docs/([^"]+)"')
# Rewrite root-relative files (LICENSE, etc.) to GitHub blob URLs
_RE_ROOT = re.compile(r'\[([^\]]+)\]\((LICENSE|CONTRIBUTING)[^)]*\)')


class _Pre(Preprocessor):
    def run(self, lines):
        text = '\n'.join(lines)
        text = _RE_GITHUB.sub(lambda m: f'[{m[1]}]({_REPO}/{m[2]})', text)
        text = _RE_DOCS.sub(lambda m: f'[{m[1]}]({m[2]})', text)
        text = _RE_DOCS_SRC.sub(r'src="\1"', text)
        text = _RE_ROOT.sub(lambda m: f'[{m[1]}]({_REPO}/{m[2]})', text)
        return text.split('\n')


class RewriteLinksExtension(Extension):
    def extendMarkdown(self, md):  # noqa: N802
        md.preprocessors.register(_Pre(md), 'rewrite_links', 50)


def makeExtension(**kwargs):  # noqa: N802
    return RewriteLinksExtension(**kwargs)
