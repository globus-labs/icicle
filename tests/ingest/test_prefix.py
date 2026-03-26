from __future__ import annotations

import pytest
from prefix import get_prefix


@pytest.mark.parametrize(
    ('path', 'level', 'expected'),
    [
        ('/a/b/c/d/e.out', 0, '/'),
        ('/a/b/c/d/e.out', 1, '/a'),
        ('/a/b/c/d/e.out', 2, '/a/b'),
        ('/a/b/c/d/e.out', 3, '/a/b/c'),
        ('/a/b/c/d/e.out', 4, '/a/b/c/d'),
        ('/a/b/c/d/e.out', 5, ''),
        ('/a', 0, '/'),
        ('/a', 1, ''),
        ('/a/b.ext', 0, '/'),
        ('/a/b.ext', 1, '/a'),
        ('/a/b.ext', 2, ''),
        ('/root/file.txt', 1, '/root'),
        ('/root/file.txt', 2, ''),
        ('/onlydir/onlysub/file', 2, '/onlydir/onlysub'),
        ('/onlydir/onlysub/file', 3, ''),
    ],
)
def test_get_prefix_various_inputs(path, level, expected):
    assert get_prefix(path, level) == expected


@pytest.mark.parametrize('level', [-3, -1, 0])
def test_get_prefix_returns_root_when_level_non_positive(level):
    assert get_prefix('/x/y/z.t', level) == '/'
