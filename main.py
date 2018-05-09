# main.py
#
from __future__ import print_function
from __future__ import division
from collections import defaultdict
import os.path
import argparse
import time
import sys
import threading
import Queue
import disamwiki


class Requests(threading.Thread):
    """Thread which gets data from Wikipedia."""
    def __init__(self, inputqueue, outputqueue):
        threading.Thread.__init__(self)
        self._inputqueue = inputqueue
        self._outputqueue = outputqueue

    def run(self):
        """ Request article from Wikipedia and put data on outputqueue. Each input will require one request, UNLESS
        we need to get extra data. This is the case if a title is passed on the queue that has the form article#section.
        If a parent article exists, link all children to parent, and vice versa.

        Each element on the inputqueue should have the following format:
            [ parent, [(link_name, title), ...] ]
        If we are requesting an article that has no parent (i.e. disambiguation page), the data would look as follows:
            [ None, [(None, title_of_disambig_page)]]
        If we are requesting the linked articles of an article, the data would look as follows:
            [ parent, [(link_name, title), ...]]
        Puts instances of Article on output queue.
        """
        while True:
            parent, linkname_title = self._inputqueue.get()

            # Split the requested titles by type. There are three types:
            #   1. simple title that requests an article (i.e Fever)
            #   2. a section in an article (i.e Fever#Types)
            #   3. a section in the containing article (#Types). If this type is passed in, parent cannot be None.
            #      A link of the form #section will be normalized to article#section where article=parent.
            # Note: The same title can be passed in with different linknames.
            simpletitles = defaultdict(list)
            titlesection = defaultdict(list)
            for linkname, title in linkname_title:
                hashtagExists = title.find('#')

                if hashtagExists == -1:  # no '#' in title
                    simpletitles[title].append(linkname)
                elif hashtagExists == 0: # '#' is first element
                    if parent is None:
                        raise ValueError('A title of the form #section was put on queue with no parent')
                    newtitle = parent.get_title() + title
                    titlesection[newtitle].append(linkname)
                elif hashtagExists > 0: # '#' is not the first element
                    titlesection[title].append(linkname)

            articles = []
            # Get articles for the simple titles
            # If there is no parent, get the entire article. Otherwise, get only the first section.
            if parent is None:
                simparticles = disamwiki.get_articles(simpletitles.keys(), section=None)
            else:
                simparticles = disamwiki.get_articles(simpletitles.keys(), section=0)
            if simparticles is not None:
                articles.extend(simparticles)

            # For each request of the form article#section
            for ts in titlesection:
                articletitle, hashtag, sectiontitle = ts.partition('#')
                article = disamwiki.get_article_fragment(articletitle, sectiontitle)
                articles.append(article)

            # Set the parent's children and set the parent of the children
            if parent is not None:
                st_article = {a.get_search_title(): a for a in articles}
                searchtitles = dict(simpletitles, **titlesection)
                for searchtitle, linknamelist in searchtitles.iteritems():
                    article = st_article[searchtitle]   # the article that was requested by titles in linknamelist
                    article.set_parent(parent)
                    parent.add_children(article, linknamelist)

            # put each individual article on output queue
            for article in articles:
                self._outputqueue.put(article)

            self._inputqueue.task_done()


## Putting pages on queue ##
def feed_disambig_title(queue, term):
    disambig_title = term + " (disambiguation)"
    feed_titles(queue, [(None, disambig_title)], parent=None)


def feed_titles(queue, request, parent=None):
    """ Breaks the requests into chunks and then puts them on the queue.

    queue: put requests on this queue
    request: list of (linkName, title) to put on queue, where linkname is the link as it appeared on the
    Wikipedia page and title is the title to request. linkName can be None.
    parent: parent of the requests (the titles are the parent article's links)
    """
    chunk_num = 5

    # populate inputqueue with data
    for requestgroup in chunks(request, chunk_num):
        queue.put([parent] + [requestgroup])


## Miscellaneous functions ##
def chunks(l, n):
    """ Yield successive n-sized chunks from a list. """
    for i in range(0, len(l), n):
        yield l[i:i+n]


def print_and_flush(string):
    """ Print string to stdout and then immediately flush the stdout stream. """
    sys.stdout.write(string.encode('utf-8'))
    sys.stdout.flush()


def print_progress(numPagesRecieved, numPagesSent):
    """ Print progress based on the number of pages recieved and the total number of pages sent. """
    percent = (numPagesRecieved/numPagesSent) * 100
    print_and_flush(u'\rArticles processed: {:5.2f}% ({}/{})'.format(percent, numPagesRecieved, numPagesSent))


## Writing articles to file ##
class FileExistsErr(Exception):
    def __init__(self, filename):
        Exception.__init__(self, filename)
        self.filename = filename


def write_files(article, overwrite=False, path=None):
    """ Recursively write articles to file. The folder name will be the title of a link (as it appears in article), and
    the *.txt files in the folder will contain the titles of the articles that were linked with given link name. If the
    same link appears multiple times in an article, folders will be titled linkname_i, where i is 2, 3, etc. Will
    raise a FileExistsErr if overwrite is False and a file exists.

    article: the root article (e.g the disambiguation page)
    overwrite: if True, overwrite files if they exist
    path: used internally for recursion
    return: None
    """
    # Write root article to file
    if article.parent is None:
        title = article.get_title().replace(" ", "_").replace('/', '-')
        foldername = title
        filename = u'{}/{}.txt'.format(foldername, title)
        # If file exists and overwrite is False raise FileExistsErr
        if overwrite is False and os.path.isfile(filename):
            raise FileExistsErr(filename)
        # Create folder if it doesn't exist
        if os.path.isdir(foldername) is False:
            os.mkdir(foldername)
        # Write file
        file = open(filename, 'w')
        file.write(article.get_plaintext().encode('utf-8'))
        file.close()
        # Set the path.
        path = article.get_title().replace(" ", "_").replace('/', '-')

    # Each article will be placed in a folder titled by the link name that linked the article. The contents of the article
    # are placed in this folder with a *.txt file of the article's contents. The *.txt file is named after the actual
    # article title as it appears on Wikipedia. If the same link appeared multiple times in an article, each folder
    # other than the first will be named linkname_i, where i is 2, 3, etc.
    for linkname, children in article.get_children().iteritems():
        normlinkname = linkname.replace(" ", "_").replace('/', '-')

        for i in range(len(children)):
            # Set the name of the folder
            if i == 0:      # first article linked by linkname
                foldername = u'{}/{}'.format(path, normlinkname)
            else:           # all other article linked by linkname
                foldername = u'{}/{}_{}'.format(path, normlinkname, i+1)

            # Set filename. If the article was found, the filename will be the title, otherwise it will be the searched title
            if children[i].missing():
                title = children[i].get_search_title().replace(" ", "_").replace('/', '-')
            else:
                title = children[i].get_title().replace(" ", "_").replace('/', '-')
            filename = u'{}/{}.txt'.format(foldername, title)

            # Create folder if it doesn't exist
            if os.path.isdir(foldername) is False:
                os.mkdir(foldername)
            # If file exists and overwrite is False raise FileExistsErr
            if overwrite is False and os.path.isfile(filename):
                raise FileExistsErr(filename)

            # Write file. If the article was never found, the file will say "DOES NOT EXIST"
            file = open(filename, 'w')
            if children[i].missing():
                file.write(u'DOES NOT EXIST')
            else:
                file.write(children[i].get_plaintext().encode('utf-8'))
            file.close()

            write_files(children[i], overwrite=overwrite, path=foldername)


def draw_article_tree(articlelist):
    import pygraphviz as pgv

    # Each key in titleToArticles is an article title, and the value is a list containing all articles with given title
    titleToArticles = dict()
    for article in articlelist:
        title = article.get_title()
        if title in titleToArticles:
            titleToArticles[title].append(article)
        else:
            titleToArticles[title] = [article]

    D = pgv.AGraph(strict=False, directed=True)
    dups = {title: artlist for (title, artlist) in titleToArticles.iteritems() if len(artlist) > 1}


    # Set nodes for articles with more than one incoming link to be orange and put them in graph.
    D.add_nodes_from(dups.keys(), fillcolor='darkorange1', style='filled')

    visited = []
    # Set the parents for all of the articles with multiple incoming links.
    for title in dups:
        duparticles = dups[title]

        for article in duparticles:
            D.add_edge(article.parent.get_title(), title, label=article.get_link_title(), color='blue')
        visited.append(title)

    # Starting at the articles with multiple links, make sure there is a path to the root node
    for title in dups:
        duparticles = dups[title]

        parents = [a.parent for a in duparticles]
        for p in parents:
            node = p
            while node.parent is not None:
                # Break if the backlinks have already been set
                if node.get_title() in visited:
                    break
                D.add_edge(node.parent.get_title(), node.get_title(), label=node.get_link_title(), color='green')
                visited.append(node.get_title())

    #
    # Graph of all nodes
    G = pgv.AGraph(strict=False, directed=True)
    duplicatetitles = [t for t in titleToArticles if len(titleToArticles[t]) > 1]
    singletitles = [t for t in titleToArticles if len(titleToArticles[t]) <= 1]
    G.add_nodes_from(duplicatetitles, fillcolor='darkorange1', style='filled')
    G.add_nodes_from(singletitles)


    for article in articlelist:
        for child in article:
            if child.get_title() in duplicatetitles:
                G.add_edge(article.get_title(), child.get_title(), label=child.get_link_title(), color='blue')
            else:
                G.add_edge(article.get_title(), child.get_title(), label=child.get_link_title())


    D.draw('dupgraph.pdf', prog='dot')
    G.draw('graph.pdf', prog='dot')


## Checking input in ArgumentParser ##
def check_int(value):
    """ Check if value is a non-negative integer. Raises a ValueError if it is not.

    value: value to check
    return: value as an int

    This function is used in the ArgumentParser to check for valid input.
    """
    try:
        ivalue = int(value)  # will raise ValueError if it cannot be converted to an int
        if ivalue < 0:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("n must be a non-negative integer or max")

    return ivalue


if __name__ == '__main__':

    # Parse the command line
    inputparser = argparse.ArgumentParser(description='Get Wikipedia disambiguation page data.')
    inputparser.add_argument('term', type=str, help='name of disambiguation page')
    inputparser.add_argument('-nl', '--num_levels', nargs='?', type=check_int, default=2,
                        help='number of levels to descend (default: 2)')
    inputparser.add_argument('-ndl', '--num_disambig_links', nargs='?', type=check_int, default=None,
                        help='number of linked pages to get for disambiguation page (default: all)')
    inputparser.add_argument('-npl', '--num_page_links', nargs='?', type=check_int, default=5,
                        help='number of linked pages to get for all pages EXCEPT disambiguation page (default: 5)')
    inputparser.add_argument('-o', '--overwrite', dest='overwrite', action='store_const', const=True,
                             default=False, help='overwrite files if they exist')

    args = inputparser.parse_args()

    # Translate command line arguments to variables used in the program
    TERM = args.term
    MAX_LEVEL = args.num_levels
    NUM_LINKS = [args.num_disambig_links] + [args.num_page_links] * (MAX_LEVEL-1)
    OVERWRITE_FILES = args.overwrite

    requests_input_queue = Queue.Queue()
    requests_output_queue = Queue.Queue()

    request_threads = 15

    # create requests threads
    for i in range(request_threads):
        producer = Requests(requests_input_queue, requests_output_queue)
        producer.daemon = True
        producer.start()

    # populate requests_input_queue with disambiguation page title
    feed_disambig_title(requests_input_queue, TERM)

    # main thread sleeps so Request threads can get started
    time.sleep(0.5)

    numpagessent = 1       # 1 since we already sent disambiguation title
    recievedarticles = []
    # event loop: gets articles output by threads
    while True:
        try:
            article = requests_output_queue.get(timeout=3)
            article.parse()
            level = article.get_level()

            # if the article was missing, continue
            if article.missing():
                requests_output_queue.task_done()
                continue

            # put the linked articles in queue for Requests threads
            if level < MAX_LEVEL:
                links = article.get_links(NUM_LINKS[level])
                numlinks = len(links)
                feed_titles(requests_input_queue, links, parent=article)
                numpagessent += numlinks

            recievedarticles.append(article)
        except Queue.Empty:  # exception will be raised when get call times out
            # Wait for all current threads to finish
            requests_input_queue.join()

            # If all threads joined and the output queue is empty, we are done
            if requests_output_queue.empty():
                print_and_flush('\n')
                break

        # print progress information
        print_progress(len(recievedarticles), numpagessent)

    # if the disambiguation page could not be found
    if len(recievedarticles) == 0:
        print_and_flush(u'No disambiguation page was found for {}\n'.format(TERM))
    else:
        # find the root node
        root = None
        for article in recievedarticles:
            if article.parent is None:
                root = article
                break

        # write pages to file
        print_and_flush('Writing files ...\n')
        try:
            write_files(root, overwrite=OVERWRITE_FILES)
        except FileExistsErr as e:
            print_and_flush('File exists: {}\n'.format(e.filename))

