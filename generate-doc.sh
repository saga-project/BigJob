#!/bin/bash

rm -rf doc
epydoc-2.7 --config epydoc.conf 

# set mime types
#find . -name "*.html" | xargs svn propset svn:mime-type text/html
#find . -name "*.css" | xargs svn propset svn:mime-type text/css
#find . -name "*.js" | xargs svn propset svn:mime-type text/javascript
