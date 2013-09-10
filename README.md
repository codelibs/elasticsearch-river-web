Elasticsearch River Reb
=======================

## Usage

    $ curl -XDELETE 'localhost:9200/_river/my_web/'

    $ curl -XPUT 'localhost:9200/_river/my_web/_meta' -d '{
        "type" : "web",
        "crawl" : {
            "url" : ["http://www.codelibs.org", "http://fess.codelibs.org/"],
            "maxDepth" : 1
        },
        "schedule" : {
            "cron" : "0 * * * * ?"
        }
    }'

