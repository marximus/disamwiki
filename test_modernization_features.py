#!/usr/bin/env python3
"""
Test modernized features in the codebase without running the full application.

This validates:
1. pathlib.Path usage
2. Context managers for file operations
3. Modern string formatting
4. HTTPS API URL
"""

import sys
import re
from pathlib import Path

def test_pathlib_usage():
    """Test that pathlib is imported and used correctly."""
    with open('main.py', 'r') as f:
        content = f.read()

    # Check pathlib import
    if 'from pathlib import Path' in content:
        print("✓ pathlib.Path is imported")
    else:
        print("✗ pathlib.Path not imported")
        return False

    # Check for Path usage
    if 'Path(' in content:
        print("✓ Path() is used in the code")
    else:
        print("✗ Path() not found")
        return False

    # Check that old os.path patterns are minimized
    if 'os.path.join' in content:
        print("⚠ Still using os.path.join (could be modernized)")
    else:
        print("✓ Not using os.path.join (using Path / operator)")

    return True

def test_context_managers():
    """Test that file operations use context managers."""
    with open('main.py', 'r') as f:
        content = f.read()

    # Count 'with open' patterns
    with_open_count = len(re.findall(r'with open\(', content))

    # Count old 'file = open' patterns
    old_open_count = len(re.findall(r'(?<!with\s)open\([^)]+\)(?!\s*as)', content))

    print(f"✓ Found {with_open_count} context manager file operations")

    if old_open_count > 0:
        print(f"⚠ Found {old_open_count} non-context-manager file operations")
        return False
    else:
        print("✓ All file operations use context managers")
        return True

def test_https_api():
    """Test that API uses HTTPS."""
    with open('disamwiki.py', 'r') as f:
        content = f.read()

    if 'https://en.wikipedia.org' in content:
        print("✓ Wikipedia API URL uses HTTPS")
        return True
    elif 'http://en.wikipedia.org' in content:
        print("✗ Wikipedia API URL uses HTTP (insecure)")
        return False
    else:
        print("⚠ Wikipedia API URL not found")
        return False

def test_modern_string_formatting():
    """Test for modern string formatting (f-strings)."""
    with open('main.py', 'r') as f:
        content = f.read()

    fstring_count = len(re.findall(r"f['\"]", content))
    format_count = len(re.findall(r'\.format\(', content))

    if fstring_count > 0:
        print(f"✓ Found {fstring_count} f-string usages (modern Python)")
    else:
        print("  Note: No f-strings found (could use more modernization)")

    if format_count > 0:
        print(f"  Found {format_count} .format() usages (acceptable)")

    return True

def test_queue_module():
    """Test that queue module (not Queue) is imported."""
    with open('main.py', 'r') as f:
        content = f.read()

    if 'import queue' in content and 'import Queue' not in content:
        print("✓ Uses 'import queue' (Python 3)")
        return True
    elif 'import Queue' in content:
        print("✗ Uses 'import Queue' (Python 2)")
        return False
    else:
        print("⚠ queue import not found")
        return False

print("="*60)
print("Testing Modernization Features")
print("="*60)
print()

all_tests_passed = True

print("1. Testing pathlib usage:")
all_tests_passed &= test_pathlib_usage()
print()

print("2. Testing context managers:")
all_tests_passed &= test_context_managers()
print()

print("3. Testing HTTPS API:")
all_tests_passed &= test_https_api()
print()

print("4. Testing modern string formatting:")
all_tests_passed &= test_modern_string_formatting()
print()

print("5. Testing queue module:")
all_tests_passed &= test_queue_module()
print()

print("="*60)
if all_tests_passed:
    print("✅ All modernization features validated successfully!")
else:
    print("⚠ Some modernization features need attention")
print("="*60)
