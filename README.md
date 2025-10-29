Create a directed graph of linked Wikipedia articles based on a disambiguation page which can be used to disambiguate words.

This is from my time as an undergraduate research assistant at the Center for Intelligent Systems and Machine Learning, University of Tennessee. Originally created in 2014, modernized in 2025.

# Requirements
- Python 3.10 or higher
- requests (https://requests.readthedocs.io/)
- mwlib (http://mwlib.readthedocs.org/en/latest/)

# Installation

## Using pip
```bash
pip install -r requirements.txt
```

To install mwlib (required):
```bash
pip install -i http://pypi.pediapress.com/simple/ mwlib
```

## For graph visualization (optional)
```bash
pip install pygraphviz
```

Note: pygraphviz requires graphviz to be installed on your system first:
- Ubuntu/Debian: `sudo apt-get install graphviz graphviz-dev`
- macOS: `brew install graphviz`
- Windows: Download from https://graphviz.org/download/

## Known Issues
- **mwlib is deprecated**: This library is no longer actively maintained. Future versions should migrate to `mwparserfromhell` for better maintenance and Python 3 support.

# Usage
python main.py [--num-levels] [--num-disambig-links] [--num-page-links] [--overwrite] disambiguation-term

# Example
Cropped example for the word "shot". For full graph, see [here](shot_duplicates.pdf).

![](shot_duplicates_crop.png)
