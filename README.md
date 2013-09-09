Elasticsearch River Reb
=======================

## Usage

    $ curl -XPUT 'localhost:9200/_river/my_web/_meta' -d '{
        "type" : "web",
        "schedule" : {
            "cron" : "0 * * * * ?"
        }
    }'

