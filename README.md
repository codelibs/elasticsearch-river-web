Elasticsearch River Web
=======================

## Overview

Elasticsearch River Web Plugin is a web crawler for Elasticsearch.
This plugin provides a feature to crawl web sites and extract the content by CSS Query.

## Installation

River Web plugin depends on Quartz plugin. 
[Quartz plugin](https://github.com/codelibs/elasticsearch-quartz) needs to be installed before installing River Web plugin.

    $ $ES_HOME/bin/plugin -install org.codelibs/elasticsearch-quartz/1.0.0

### Download River Web Plugin

You can download River Web Plugin from [here](https://oss.sonatype.org/content/repositories/snapshots/org/codelibs/elasticsearch-river-web/).

### Install River Web Plugin

    $ $ES_HOME/bin/plugin -url file:$DOWNLOAD_DIR/elasticsearch-river-web-*.zip -install river-web

### Create Index For Crawling

River Web Plugin needs several indexes for web crawling.
Therefore, you need to create them before starting the crawl.
Type the following commands to create the index:

    $ curl -XPUT 'localhost:9200/robot/'

## Usage

### Register Crawl Data

A crawling configuration is created by registering a river as below:

    $ curl -XPUT 'localhost:9200/_river/my_web/_meta' -d "{
        \"type\" : \"web\",
        \"crawl\" : {
            \"index\" : \"web\",
            \"url\" : [\"http://www.codelibs.org/\", \"http://fess.codelibs.org/\"],
            \"includeFilter\" : [\"http://www.codelibs.org/.*\", \"http://fess.codelibs.org/.*\"],
            \"maxDepth\" : 3,
            \"maxAccessCount\" : 100,
            \"numOfThread\" : 5,
            \"interval\" : 1000,
            \"target\" : [
              {
                \"pattern\" : {
                  \"url\" : \"http://www.codelibs.org/.*\",
                  \"mimeType\" : \"text/html\"
                },
                \"properties\" : {
                  \"title\" : {
                    \"text\" : \"title\"
                  },
                  \"body\" : {
                    \"text\" : \"body\"
                  },
                  \"bodyAsHtml\" : {
                    \"html\" : \"body\"
                  },
                  \"projects\" : {
                    \"text\" : \"ul.nav-list li a\",
                    \"isArray\" : true
                  }
                }
              },
              {
                \"pattern\" : {
                  \"url\" : \"http://fess.codelibs.org/.*\",
                  \"mimeType\" : \"text/html\"
                },
                \"properties\" : {
                  \"title\" : {
                    \"text\" : \"title\"
                  },
                  \"body\" : {
                    \"text\" : \"body\",
                    \"trimSpaces\" : true
                  },
                  \"menus\" : {
                    \"text\" : \"ul.nav-list li a\",
                    \"isArray\" : true
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

| Property                          | Type    | Description                                     |
|:----------------------------------|:-------:|:------------------------------------------------|
| crawl.index                       | string  | Stored index name.                              |
| crawl.url                         | array   | Start point of URL for crawling.                |
| crawl.includeFilter               | array   | White list of URL for crawling.                 |
| crawl.excludeFilter               | array   | Black list of URL for crawling.                 |
| crawl.maxDepth                    | int     | Depth of crawling documents.                    |
| crawl.maxAccessCount              | int     | The number of crawling documents.               |
| crawl.numOfThread                 | int     | The number of crawler threads.                  |
| crawl.interval                    | int     | Interval time (ms) to crawl documents.          |
| crawl.incremental                 | boolean | Incremental crawling.                           |
| crawl.overwrite                   | boolean | Delete documents of old duplicated url.         |
| crawl.target.urlPattern           | string  | URL pattern to extract contents by CSS Query.   |
| crawl.target.properties.name      | string  | "name" is used as a property name in the index. |
| crawl.target.properties.name.text | string  | CSS Query for the property value.               |
| crawl.target.properties.name.html | string  | CSS Query for the property value.               |
| schedule.cron                     | string  | Cron format to start a crawler.                 |


### Unregister Crawl Data

If you want to stop the crawler, type as below: (replace my\_web with your river name)

    $ curl -XDELETE 'localhost:9200/_river/my_web/'

## Examples

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
                    \"text\" : \"h1.headline\"
                  },
                  \"content\" : {
                    \"text\" : \"section.mediacontentstory div.body p\"
                  }
                }
              }
            ]
        },
        \"schedule\" : {
            \"cron\" : \"0 0 * * * ?\"
        }
    }"


## Others

### Use Multibyte Characters

An example in Japanese environment is below.
First, put some configuration file into conf directory of Elasticsearch.

    $ cd $ES_HOME/conf    # ex. /etc/elasticsearch if using rpm package
    $ sudo wget https://raw.github.com/codelibs/fess-server/master/src/tomcat/solr/core1/conf/mapping_ja.txt
    $ sudo wget http://svn.apache.org/repos/asf/lucene/dev/trunk/solr/example/solr/collection1/conf/lang/stopwords_ja.txt 

and then create web index with analyzers for Japanese.
(If you want to use uni-gram, remove cjk_bigram in filter)

    $ curl -XPUT "localhost:9200/web" -d "
    {
      \"settings\" : {
        \"analysis\" : {
          \"analyzer\" : {
            \"default\" : {
              \"type\" : \"custom\",
              \"char_filter\" : [\"mappingJa\"],
              \"tokenizer\" : \"standard\",
              \"filter\" : [\"word_delimiter\", \"lowercase\", \"cjk_width\", \"cjk_bigram\"]
            }
          },
          \"char_filter\" : {
            \"mappingJa\": {
              \"type\" : \"mapping\",
              \"mappings_path\" : \"mapping_ja.txt\"
            }
          },
          \"filter\" : {
            \"stopJa\" : {
              \"type\" : \"stop\",
              \"stopwords_path\" : \"stopwords_ja.txt\"
            }
          }
        }
      }
    }"

