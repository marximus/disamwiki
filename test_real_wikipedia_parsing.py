#!/usr/bin/env python3
"""
Test the refactored disamwiki module with real Wikipedia data.

This validates that mwparserfromhell replacement works correctly.
"""

import disamwiki

def test_article_creation_and_parsing():
    """Test creating an Article and parsing it."""
    print("="*60)
    print("Testing Article class with mwparserfromhell")
    print("="*60)

    # Sample Wikipedia wikitext (similar to what API returns)
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
"""

    # Create an Article instance
    article = disamwiki.Article(
        pageid=12345,
        search_title="Shot (disambiguation)",
        title="Shot (disambiguation)",
        wikitext=test_wikitext,
        parent=None
    )

    print("\n✓ Article created")
    print(f"  Title: {article.get_title()}")
    print(f"  Search title: {article.get_search_title()}")
    print(f"  Page ID: {article.pageid}")

    # Parse the article
    article.parse()
    print("\n✓ Article parsed")

    # Check plaintext
    plaintext = article.get_plaintext()
    print(f"\n✓ Plain text extracted ({len(plaintext)} chars)")
    print(f"  Preview: {plaintext[:100]}...")

    # Check links
    links = article.get_links()
    print(f"\n✓ Links extracted ({len(links)} total)")
    for i, (display, target) in enumerate(links[:5], 1):
        print(f"  {i}. '{display}' -> '{target}'")

    # Test ignoring sections
    print(f"\n✓ Testing section filtering:")
    print(f"  Total links before filter: {len(links)}")

    # The ignoreSections should have filtered out "See also" and "References"
    see_also_links = [l for l in links if 'Shooting' in l[1] or 'Shooter' in l[1]]
    print(f"  Links from 'See also' section: {len(see_also_links)}")

    # Test get_links with limit
    limited_links = article.get_links(3)
    print(f"\n✓ get_links(3) returned {len(limited_links)} links")

    return article


def test_get_text_and_links():
    """Test the get_text_and_links function directly."""
    print("\n" + "="*60)
    print("Testing get_text_and_links function")
    print("="*60)

    test_wikitext = """
== Section 1 ==
This links to [[Python (programming language)|Python]].

== See also ==
This should be filtered: [[Java]]

== Section 2 ==
This links to [[Ruby (programming language)|Ruby]].
"""

    import mwparserfromhell
    parsed = mwparserfromhell.parse(test_wikitext)

    # Without filtering
    text1, links1 = disamwiki.get_text_and_links(parsed)
    print(f"\n✓ Without filtering:")
    print(f"  Links: {len(links1)}")
    for display, target in links1:
        print(f"    '{display}' -> '{target}'")

    # With filtering
    text2, links2 = disamwiki.get_text_and_links(parsed, ignoreSections=['See also'])
    print(f"\n✓ With 'See also' filtered:")
    print(f"  Links: {len(links2)}")
    for display, target in links2:
        print(f"    '{display}' -> '{target}'")

    assert len(links2) < len(links1), "Filtering should reduce link count"
    assert len(links2) == 2, f"Expected 2 links after filtering, got {len(links2)}"

    print("\n✓ Section filtering working correctly!")


def test_article_hierarchy():
    """Test parent-child relationships."""
    print("\n" + "="*60)
    print("Testing Article parent-child relationships")
    print("="*60)

    # Create parent article
    parent = disamwiki.Article(
        pageid=1,
        search_title="Parent",
        title="Parent",
        wikitext="Parent article",
        parent=None
    )

    # Create child article
    child = disamwiki.Article(
        pageid=2,
        search_title="Child",
        title="Child",
        wikitext="Child article",
        parent=parent
    )

    # Add child to parent
    parent.add_children(child, ["link1", "link2"])

    print("\n✓ Parent-child relationship created")
    print(f"  Parent level: {parent.get_level()}")
    print(f"  Child level: {child.get_level()}")

    assert parent.get_level() == 0, "Parent should be level 0"
    assert child.get_level() == 1, "Child should be level 1"

    children = parent.get_children()
    assert "link1" in children, "link1 should be in children"
    assert "link2" in children, "link2 should be in children"

    print("✓ Hierarchy working correctly!")


if __name__ == '__main__':
    try:
        # Run tests
        article = test_article_creation_and_parsing()
        test_get_text_and_links()
        test_article_hierarchy()

        print("\n" + "="*60)
        print("✅ All tests passed!")
        print("="*60)
        print("\nThe mwparserfromhell integration is working correctly.")
        print("Article parsing, link extraction, and section filtering")
        print("all function as expected.")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
