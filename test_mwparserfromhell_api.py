#!/usr/bin/env python3
"""
Test script to understand mwparserfromhell API and verify it can replace mwlib.

This script tests:
1. Parsing wikitext
2. Extracting links (with display text vs target)
3. Extracting plain text
4. Handling sections
5. Ignoring specific sections
"""

import mwparserfromhell

# Sample Wikipedia-style wikitext
test_wikitext = """
'''Shot''' may refer to:

== Projectiles ==
* [[Shotgun shell|Shot]] (pellet), fragments used as ammunition
* [[Shot (ice hockey)]], a shot in ice hockey
* [[Screenshot]], an image of a computer screen

== Beverages ==
* [[Shot (drink)]], a small serving of a beverage
* [[Shot glass]], a drinking glass

== See also ==
* [[Shooting]]
* [[Shooter (disambiguation)]]

== References ==
Some references here.

== External links ==
* [http://example.com Example]
"""

def extract_text_and_links(wikicode, ignore_sections=None):
    """
    Extract plain text and links from parsed wikicode.

    Returns:
        text: plain text string
        links: list of (link_display_text, link_target) tuples
    """
    if ignore_sections is None:
        ignore_sections = []

    links = []
    text_parts = []

    # Get sections
    sections = wikicode.get_sections(include_headings=True)

    for section in sections:
        # Check if this section should be ignored
        headings = section.filter_headings()
        if headings:
            section_title = headings[0].title.strip()
            if section_title in ignore_sections:
                # Add section heading but skip content
                text_parts.append(str(headings[0]))
                text_parts.append('\n')
                continue

        # Extract links from this section
        for link in section.filter_wikilinks():
            target = str(link.title)
            # Display text is either the custom text or the target
            display = str(link.text) if link.text else target
            links.append((display, target))

        # Get plain text from section
        text_parts.append(section.strip_code())

    plaintext = ''.join(text_parts)
    return plaintext, links


# Test the function
print("="*60)
print("Testing mwparserfromhell API")
print("="*60)

parsed = mwparserfromhell.parse(test_wikitext)
print("\n✓ Parsing successful")
print(f"  Type: {type(parsed)}")

# Extract without ignoring sections
plaintext, links = extract_text_and_links(parsed)
print(f"\n✓ Extracted {len(links)} links")
for display, target in links[:3]:
    print(f"  '{display}' -> '{target}'")

print(f"\n✓ Plain text preview:")
print(plaintext[:200])

# Extract while ignoring certain sections
ignore = ['See also', 'References', 'External links']
plaintext2, links2 = extract_text_and_links(parsed, ignore_sections=ignore)
print(f"\n✓ After ignoring {ignore}:")
print(f"  Links reduced from {len(links)} to {len(links2)}")
print(f"  Text length reduced from {len(plaintext)} to {len(plaintext2)}")

# Test individual link parsing
print("\n✓ Testing link variations:")
test_links = [
    "[[Simple link]]",
    "[[Target|Display text]]",
    "[[Article#Section]]",
    "[[Article#Section|Custom text]]"
]

for link_text in test_links:
    parsed_link = mwparserfromhell.parse(link_text)
    for link in parsed_link.filter_wikilinks():
        display = str(link.text) if link.text else str(link.title)
        print(f"  {link_text:35} -> display='{display}', target='{link.title}'")

print("\n" + "="*60)
print("✅ mwparserfromhell API validated successfully!")
print("="*60)
print("\nKey differences from mwlib:")
print("  - mwparserfromhell.parse() instead of uparser.parseString()")
print("  - filter_wikilinks() instead of traversing tree for ArticleLink")
print("  - link.title and link.text instead of node.target and node.children")
print("  - strip_code() instead of asText()")
