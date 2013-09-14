Elasticsearch River Web
=======================

## Overview

Elasticsearch River Web Plugin provides a feature to crawl web sites and store the content by XPath.

## Installation

### Download River Web Plugin


You can download River Web Plugin from [here](http://maven.codelibs.org/org/codelibs/elasticsearch-river-web/).

### Install River Web Plugin

    $ $ES_HOME/bin/plugin -url file:$DOWNLOAD_DIR/elasticsearch-river-web-*.zip -install river-web

### Create Index For Crawling

River Web Plugin needs several indexes for web crawling.
Therefore, you need to create them before starting the crawl.
Type the following commands to create the indexes:

    $ curl -XPUT 'localhost:9200/robot_queue/'
    $ curl -XPUT 'localhost:9200/robot_data/'
    $ curl -XPUT 'localhost:9200/robot_filter/'

## Usage

### Register Crawl Data

A crawling configuration is created by registering a river as below:

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

The configuration is:

| Property | Type | Description |
|-|-|-|
| crawl.index | string | Stored index name. |
| crawl.url | array | Start point of URL for crawling. |
| crawl.includeFilter | array | White list of URL for crawling. |
| crawl.excludeFilter | array | Black list of URL for crawling. |
| crawl.maxDepth | int | Depth of crawling documents. |
| crawl.maxAccessCount | int | The number of crawling documents. |
| crawl.numOfThread | int | The number of crawler threads. |
| crawl.interval | int | Interval time (ms) to crawl documents. |
| crawl.target.urlPattern | string | URL pattern to extract contents by XPath. |
| crawl.target.properties.name | string | "name" is used as a property name in the index. |
| crawl.target.properties.name.path | string | XPath for the property value. |
| crawl.target.properties.name.writeAsXml | boolean | Write a value as XML if true. |
| schedule.cron | string | cron format to start a crawler. |


### Unregister Crawl Data

If you want to stop the crawler, type as below: (replace my\_web with your river name)

    $ curl -XDELETE 'localhost:9200/_river/my_web/'

## Example

### Aggregate a title/content from news.yahoo.com

    $ curl -XDELETE 'localhost:9200/_river/yahoo_com/'
    $ curl -XPUT 'localhost:9200/_river/yahoo_com/_meta' -d "{
        \"type\" : \"web\",
        \"crawl\" : {
            \"index\" : \"web\",
            \"url\" : [\"http://news.yahoo.com/\"],
            \"includeFilter\" : [\"http://news.yahoo.com/.*\"],
            \"maxDepth\" : 1,
            \"maxAccessCount\" : 100,
            \"numOfThread\" : 3,
            \"interval\" : 3000,
            \"target\" : [
              {
                \"urlPattern\" : \"http://news.yahoo.com/.*html\",
                \"properties\" : {
                  \"title\" : {
                    \"path\" : \"//H1[@class='headline']\"
                  },
                  \"content\" : {
                    \"path\" : \"//SECTION[@id='mediacontentstory']/DIV/DIV[@class='body clearfix']/P\"
                  }
                }
              }
            ]
        },
        \"schedule\" : {
            \"cron\" : \"0 0 * * * ?\"
        }
    }"


