Elasticsearch River Web
=======================

## Overview

Elasticsearch River Web Plugin provides a feature for crawling web sites and storing the contents by XPath.

## Installation

### Download River Web Plugin


You can download River Web Plugin from [here](http://maven.codelibs.org/org/codelibs/elasticsearch-river-web/).

### Install River Web Plugin

    $ $ES_HOME/bin/plugin -url file:$DOWNLOAD_DIR/elasticsearch-river-web-*.zip -install river-web

### Create Index For Crawling

    $ curl -XPUT 'localhost:9200/robot_queue/'
    $ curl -XPUT 'localhost:9200/robot_data/'
    $ curl -XPUT 'localhost:9200/robot_filter/'

## Usage

### Register Crawl Data

    $ curl -XPUT 'localhost:9200/_river/my_web/_meta' -d "{
        \"type\" : \"web\",
        \"crawl\" : {
            \"index\" : \"web\",
            \"url\" : [\"http://www.codelibs.org/\", \"http://fess.codelibs.org/\"],
            \"includeFilter\" : [\"http://www.codelibs.org/.*\", \"http://fess.codelibs.org/.*\"],
            \"maxDepth\" : 1,
            \"maxAccessCount\" : 100,
            \"numOfThread\" : 5,
            \"interval\" : 1000,
            \"target\" : [
              {
                \"urlPattern\" : \"http://www.codelibs.org/.*\",
                \"properties\" : {
                  \"title\" : {
                    \"path\" : \"//TITLE\"
                  },
                  \"body\" : {
                    \"path\" : \"//BODY\"
                  },
                  \"bodyAsXml\" : {
                    \"path\" : \"//BODY\",
                    \"writeAsXml\" : true
                  },
                  \"projects\" : {
                    \"path\" : \"//UL[@class='nav nav-list']/LI/A\"
                  }
                }
              },
              {
                \"urlPattern\" : \"http://fess.codelibs.org/.*\",
                \"properties\" : {
                  \"title\" : {
                    \"path\" : \"//TITLE\"
                  },
                  \"body\" : {
                    \"path\" : \"//BODY\"
                  },
                  \"menus\" : {
                    \"path\" : \"//UL[@class='nav nav-list']/LI/A\"
                  }
                }
              }
            ]
        },
        \"schedule\" : {
            \"cron\" : \"0 * * * * ?\"
        }
    }"

### Unregister Crawl Data

    $ curl -XDELETE 'localhost:9200/_river/my_web/'

