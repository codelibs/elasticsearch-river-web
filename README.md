Elasticsearch River Reb
=======================

## Installation

### Install River Web Plugin

### Create Index For Crawling

    $ curl -XPUT 'localhost:9200/robot_queue/'
    $ curl -XPUT 'localhost:9200/robot_data/'
    $ curl -XPUT 'localhost:9200/robot_filter/'

## Usage

### Register Crawl Data

    $ curl -XPUT 'localhost:9200/_river/my_web/_meta' -d '{
        "type" : "web",
        "crawl" : {
            "index" : "web",
            "url" : ["http://www.codelibs.org", "http://fess.codelibs.org/"],
            "maxDepth" : 1,
            "target" : [
              {
                "urlPattern" : "http://www.codelibs.org.*",
                "properties" : {
                  "title" : "//TITLE",
                  "body" : "//BODY",
                  "projects" : "//UL[@class='nav-list']/LI/A"
                }
              },
              {
                "urlPattern" : "http://fess.codelibs.org.*",
                "properties" : {
                  "title" : "//TITLE",
                  "body" : "//BODY",
                  "menus" : "//UL[@class='nav-list']/LI/A"
                }
              }
            ]
        },
        "schedule" : {
            "cron" : "0 * * * * ?"
        }
    }'

### Unregister Crawl Data

    $ curl -XDELETE 'localhost:9200/_river/my_web/'

