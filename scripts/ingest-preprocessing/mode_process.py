#!/usr/bin/env python3
from __future__ import annotations

import itertools
import stat


def sym_to_mode(s: str) -> int:
    if len(s) != 10:
        raise ValueError('Symbolic mode must be exactly 10 characters')

    type_bits = {
        '-': stat.S_IFREG,
        'd': stat.S_IFDIR,
        'l': stat.S_IFLNK,
        'c': stat.S_IFCHR,
        'b': stat.S_IFBLK,
        's': stat.S_IFSOCK,
        'p': stat.S_IFIFO,
    }
    if s[0] not in type_bits:
        raise ValueError(f'Invalid file type char: {s[0]}')
    mode = type_bits[s[0]]

    perms = s[1:]

    # allowed char sets per position (0..8 = u:rwx g:rwx o:rwx)
    allowed = [
        set('r-'),
        set('w-'),
        set('x-') | set('sS'),
        set('r-'),
        set('w-'),
        set('x-') | set('sS'),
        set('r-'),
        set('w-'),
        set('x-') | set('tT'),
    ]
    for i, ch in enumerate(perms):
        if ch not in allowed[i]:
            raise ValueError(
                f"Invalid char '{ch}' at pos {i + 1} in '{s}' "
                f'(allowed: {"".join(sorted(allowed[i]))})',
            )

    # build bits
    triples = [
        (0, stat.S_ISUID),  # user block start, suid
        (3, stat.S_ISGID),  # group block start, sgid
        (6, stat.S_ISVTX),  # other block start, sticky
    ]
    for idx, (start, special_bit) in enumerate(triples):
        r, w, x = perms[start : start + 3]
        val = 0
        if r == 'r':
            val += 4
        if w == 'w':
            val += 2

        if x in ('x', 's', 't'):
            val += 1
        if x in ('s', 'S') and idx == 0:
            mode |= stat.S_ISUID
        if x in ('s', 'S') and idx == 1:
            mode |= stat.S_ISGID
        if x in ('t', 'T') and idx == 2:
            mode |= stat.S_ISVTX

        mode |= val << (6 - 3 * idx)

    return mode


# ---------- tests ----------


def explicit_tests():
    cases = [
        # sym                full (type+special+perms)   special+perms
        ('-rw-r--r--', 0o100644, 0o0644),
        ('-rwsr-xr-x', 0o104755, 0o4755),  # suid
        ('drwxr-sr-x', 0o042755, 0o2755),  # sgid
        ('drwxrwxrwt', 0o041777, 0o1777),  # sticky
        ('-rwxr-sr--', 0o102754, 0o2754),  # sgid on reg file (FIXED)
        ('-r-Srwxr-x', 0o104475, 0o4475),  # suid w/o user x (uppercase S)
        ('drwxrwx--T', 0o041770, 0o1770),  # sticky w/o other x (uppercase T)
        ('-rwsr-sr-x', 0o106755, 0o6755),  # suid + sgid
    ]
    for sym, full_oct, sp_oct in cases:
        m = sym_to_mode(sym)
        assert stat.filemode(m) == sym, f'{sym}: round-trip mismatch'
        assert (m & 0o177777) == full_oct, (
            f'{sym}: full bits mismatch {oct(m)} != {oct(full_oct)}'
        )
        assert (m & 0o7777) == sp_oct, (
            f'{sym}: perm+special mismatch {oct(m & 0o7777)} != {oct(sp_oct)}'
        )
        print(f'[OK] {sym} -> {m} (oct: {oct(m)})')


def roundtrip_sweep():
    file_types = ['-', 'd', 'l']
    R = ['r', '-']
    W = ['w', '-']
    UX = ['x', '-', 's', 'S']
    GX = ['x', '-', 's', 'S']
    OX = ['x', '-', 't', 'T']
    total = 0
    for f in file_types:
        for u in itertools.product(R, W, UX):
            for g in itertools.product(R, W, GX):
                for o in itertools.product(R, W, OX):
                    sym = f + ''.join(u) + ''.join(g) + ''.join(o)
                    m = sym_to_mode(sym)
                    back = stat.filemode(m)
                    assert sym == back, f'FAILED: {sym} -> {oct(m)} -> {back}'
                    total += 1
    print(f'[OK] Round-trip verified on {total} combinations.')


if __name__ == '__main__':
    explicit_tests()
    roundtrip_sweep()
    print('\nAll tests passed.')
