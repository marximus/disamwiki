#!/usr/bin/env python3
"""
Test the syntax of main.py and disamwiki.py without executing mwlib imports.

This validates that our Python 3 migration didn't introduce syntax errors.
"""

import sys
import ast

def test_file_syntax(filename):
    """Parse a Python file and check for syntax errors."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            source = f.read()

        # Try to parse the file
        ast.parse(source, filename=filename)
        print(f"✓ {filename}: Valid Python 3 syntax")
        return True
    except SyntaxError as e:
        print(f"✗ {filename}: Syntax error at line {e.lineno}: {e.msg}")
        return False
    except Exception as e:
        print(f"✗ {filename}: Error: {e}")
        return False

def validate_python3_features(filename):
    """Check that Python 3 features are used correctly."""
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    issues = []

    # Check for Python 2 specific imports
    if 'from __future__ import' in content:
        issues.append(f"  ⚠ Found '__future__' import (Python 2 compatibility)")

    # Check for old Queue module
    if 'import Queue' in content:
        issues.append(f"  ✗ Found 'import Queue' (should be 'import queue')")

    # Check for iteritems (Python 2)
    if '.iteritems()' in content:
        issues.append(f"  ✗ Found '.iteritems()' (should be '.items()')")

    # Check for old string handling
    if '.encode(\'utf-8\')' in content and 'file.write' in content:
        # This is acceptable for file writes in some cases, but with encoding='utf-8' it's not needed
        pass

    if issues:
        print(f"  Issues found in {filename}:")
        for issue in issues:
            print(issue)
        return False
    else:
        print(f"  ✓ {filename}: No Python 2 legacy code found")
        return True

print("="*60)
print("Testing Python 3 syntax and compatibility")
print("="*60)
print()

all_good = True

# Test syntax
all_good &= test_file_syntax('main.py')
all_good &= test_file_syntax('disamwiki.py')

print()

# Validate Python 3 features
all_good &= validate_python3_features('main.py')
all_good &= validate_python3_features('disamwiki.py')

print()
print("="*60)
if all_good:
    print("✅ All syntax validation passed!")
else:
    print("⚠ Some issues found (see above)")
print("="*60)
