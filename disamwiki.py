# DisamWiki.py
# Maurice Marx
#
# Use Wikipedia to get ambiguous content


from __future__ import print_function
from collections import defaultdict
import requests
from mwlib import parser, uparser


API_URL = 'http://en.wikipedia.org/w/api.php'
USER_AGENT = 'DisambigWiki (www.utk.edu)'


class Article:
    # ignore sections with these titles
    ignoreSections=('See also', 'References', 'Further reading', 'External links', 'Footnotes', 'Notes',
                    'Other', 'Other uses')

    def __init__(self, pageid, search_title, title, wikitext, parent=None):
        self.parent = parent
        self.search_title = search_title
        self.title = title
        self.pageid = pageid
        self.wikitext = wikitext
        self.children = defaultdict(list) # key is a link name and value is list of aritcles linked from link name
        self.parsetree = None
        self.links = None
        self.plaintext = None


    def __iter__(self):
        for x in self.children:
            yield x


    def parse(self):
        """ Create a parse tree and then extract data for article from it. """
        # if the page was missing, return
        if self.missing():
            return

        self.parsetree = uparser.parseString(title=self.title, raw=self.wikitext)

        text, links = get_text_and_links(self.parsetree, self.ignoreSections)
        plaintext = u''.join(text)
        # Remove newlines and spaces that occur at beginning of text
        self.plaintext = plaintext.lstrip(' \n')
        self.links = links


    def set_parent(self, parent):
        self.parent = parent


    def get_children(self, childrenonly=False):
        """ If childrenonly is True, return a list of the article's children. Otherwise, return the dictionary
        that has the form:   linkname -> [list of articles linked by linkname]
        """
        if childrenonly is False:
            return self.children
        else:
            children = [child for childlist in self.children.values() for child in childlist]
            return children


    def add_children(self, childarticle, linknames):
        """ The childarticle was linked with names in linknames. It is important to note
        that different linknames will share the same instance of childarticle.
        linknames - list of link names
        """
        for linkname in linknames:
            self.children[linkname].append(childarticle)


    def get_title(self):
        """ Returns the title of the Wikipedia article, as it appears on Wikipedia. """
        return self.title


    def get_search_title(self):
        """ Returns the title that was used to search for the article. It is the title
        before any redirects or normalizations.
        """
        return self.search_title


    def get_links(self, numlinks=None):
        """Returns links from article. If numlinks=None return all links."""
        return self.links[:numlinks]


    def get_level(self):
        """ Get level of page in page hierarchy. level 0 = root node """
        page = self
        level = 0
        while page.parent is not None:
            page = page.parent
            level += 1

        return level


    def get_plaintext(self):
        """ Return a string with plaintext for all sections. """
        return self.plaintext


    def get_hierarchy(self):
        """ Return a string showing the page in the hierarchy.

        Format of string: i.e. parent1 --> parent2 --> this page
        """
        page = self
        reversedhierarchy = [page.get_title() or page.get_search_title()]
        while page.parent is not None:
            reversedhierarchy.append(page.parent.get_title())
            page = page.parent
        # reverse the list so that the order of parents is logical
        hierarchy = reversedhierarchy[::-1]

        return ' --> '.join(hierarchy)


    def missing(self):
        return self.pageid < 0


##########################
# Getting Wikipedia Data #
##########################
def get_articles(search_titles, section=None):
#     """ Get data from Wikipedia.
#
#     search_titles - a list of search_titles
#     section - the section number to get content for (default: all)
#
#     Returns a list of all the articles. If an article is not found, an article with pageid < 0 will be created.
#     """
    if len(search_titles) == 0:
        return None

    params = dict(action='query', prop='revisions', rvexpandtemplates='', rvprop='content', redirects='')
    params['titles'] = u'|'.join(search_titles)
    if section is not None:
        params['rvsection'] = section

    # get data from Wikipedia API
    request = _wikirequest(params)
    query = request['query']

    norms = dict((n['from'], n['to']) for n in query.get('normalized', []))
    redirects = dict((r['from'], r) for r in query.get('redirects', []))
    articledata = []

    # Resolve normalizations and redirects
    for search_title in search_titles:
        article = {'search_title': search_title, 'title': search_title, 'tofragment': None, 'pageid':None, 'wikitext':None}
        # Update title to normalized title if it exists
        if search_title in norms:
            article['title'] = norms[search_title]

        # Update title to redirect title if it exists. Also update tofragment if it existed in redirect.
        if article['title'] in redirects:
            old_title = article['title']
            article['title'] = redirects[old_title]['to']
            # If there is a redirect to a fragment
            if 'tofragment' in redirects[old_title]:
                article['tofragment'] = redirects[old_title]['tofragment']

        articledata.append(article)

    for pageidstr, wikipage in query['pages'].items():
        pageid = int(pageidstr)
        # for all articles that have the title of the wikipage
        articlematch = [a for a in articledata if a['title'] == wikipage['title']]
        for article in articlematch:
            article['pageid'] = pageid

        # if page is missing, 'missing' will appear in the page and the pageid < 0
        if 'missing' in wikipage:
            continue

        # get the wikitext
        if 'revisions' in wikipage:
            if '*' in wikipage['revisions'][0]:
                for article in articlematch:
                    article['wikitext'] = wikipage['revisions'][0]['*']

    # Create Article instances from article data
    returnarticles = []
    for ar in articledata:
        # If the article was a redirect to a fragment, we need to make two request the article fragment
        if ar['tofragment'] is not None:
            article = get_article_fragment(ar['title'], ar['tofragment'])
            article.search_title = ar['search_title']
        else:
            article = Article(ar['pageid'], ar['search_title'], ar['title'], ar['wikitext'])
        returnarticles.append(article)

    return returnarticles


def get_article_section_number(articletitle, sectiontitle):
    """ Returns the section number of sectiontitle in articletitle, or None if it does not exist.

        articletitle: title of article that contains the section
        sectiontitle: title of section
    """
    params = dict(action='parse', prop='sections', redirects='', page=articletitle)

    query = _wikirequest(params)

    for section in query['parse']['sections']:
        if section['line'] == sectiontitle:
            return section['index']

    return None


def get_article_fragment(articletitle, fragmenttitle):
    """ Return the specified fragment/section of the given article. The search_title of the article will be set to
    articletitle#fragmenttitle. If no such fragment exists, return an article with pageid < 0.

    articletitle: title of article
    fragmenttitle: title of fragment in article
    """
    # Get the section number of the fragment from Wikipedia
    sectionnum = None

    params = dict(action='parse', prop='sections', redirects='', page=articletitle)
    query = _wikirequest(params)
    for section in query['parse']['sections']:
        if section['line'] == fragmenttitle:
            sectionnum = section['index']

    title = u'{}#{}'.format(articletitle, fragmenttitle)
    # If the section number could not be found, return an article with pageid < 0
    if sectionnum is None:
        return Article(pageid=-1, search_title=title, title=None, wikitext=None, parent=None)

    newarticle = get_articles([articletitle], sectionnum)[0]
    newarticle.title = title
    newarticle.search_title = title

    return newarticle


def _wikirequest(params):
    """
    Makes a request to the Wikipedia API with the given parameters.
    Returns the parsed JSON text.
    """
    global USER_AGENT
    global API_URL

    headers = {'USER_AGENT': USER_AGENT}
    params['format'] = 'json'

    result = requests.get(API_URL, params=params, headers=headers)

    return result.json()


ignoreTypes = (parser.Table, parser.ImageLink, parser.CategoryLink, parser.NamespaceLink, parser.TagNode)
def get_text_and_links(node, ignoreSections=None, text=None, links=None):
    """ Extract the text and links from the parsetree that has node as root. This function modifies the
    input parse tree. The links are returned as a tuple (linkname, linktarget), where linkname is how the
    link appeared in the article and linktarget is that target article of the link.

    node: root of parsetree
    ignoreSections: sections to ignore when extracting from the parsetree
    """
    if text is None:
        text = []
    if links is None:
        links = []

    if type(node) is parser.Text:
        text.append(node.asText())
    elif type(node) is parser.Section:
        # The first element in children contains the caption of the section as a Node
        # instance with 0 or more children. Subsequent children are elements following
        # the section heading.
        headingNode = node.children.pop(0)
        sectiontitle = headingNode.asText()
        equalsign = '=' * node.level
        text.append(u'{} {} {}'.format(equalsign, sectiontitle, equalsign))
        # If the section is to be ignored, remove all of the section's children an insert a newline into the text
        if sectiontitle in ignoreSections:
            text.append('\n')
            node.children = []
    elif type(node) is parser.ArticleLink:
        # Article link has style [[target]] in wikitext
        if len(node.children) == 0:
            text.append(node.target)
            links.append((node.target, node.target))
        else:
            linkname = u''
            for c in node.allchildren():
                if isinstance(c, parser.Text):
                    linkname += c.asText()
            links.append((linkname, node.target))
            # links.append((node.asText(), node.target))

    if type(node) not in ignoreTypes:
        for child in node.children:
            get_text_and_links(child, ignoreSections, text, links)

    return text, links