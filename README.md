Elasticsearch River Web
=======================

## Overview

Elasticsearch River Web is a web crawler application for Elasticsearch.
This application provides a feature to crawl web sites and extract the content by CSS Query.
(As of version 1.5, River Web is not Elasticsearch plugin)

If you want to use Full Text Search Server, please see [Fess](https://github.com/codelibs/fess "Fess").

## Version

| River Web | Tested on ES  | Download |
|:---------:|:-------------:|:--------:|
| master    | 2.3.X         | [Snapshot](http://maven.codelibs.org/org/codelibs/river-web/ "Snapshot") |
| 2.0.2     | 2.3.1         | [Download](https://github.com/codelibs/elasticsearch-river-web/releases/tag/river-web-2.0.2 "2.0.2") |
| 2.0.1     | 2.2.0         | [Download](https://github.com/codelibs/elasticsearch-river-web/releases/tag/river-web-2.0.1 "2.0.1") |
| 2.0.0     | 2.1.2         | [Download](https://github.com/codelibs/elasticsearch-river-web/releases/tag/river-web-2.0.0 "2.0.0") |

For old version, see [README\_ver1.md](https://github.com/codelibs/elasticsearch-river-web/blob/master/README_ver1.md "README_ver1.md") or [README\_ver1.5.md](https://github.com/codelibs/elasticsearch-river-web/blob/master/README_ver1.5.md "README_ver1.5.md").

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-river-web/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install River Web 

#### Zip File

    $ unzip elasticsearch-river-web-[VERSION].zip

#### Tar.GZ File

    $ tar zxvf elasticsearch-river-web-[VERSION].tar.gz

## Usage

### Create Index To Store Crawl Data

An index for storing crawl data is needed before starting River Web.
For example, to store data to "webindex/my_web", create it as below:

    $ curl -XPUT 'localhost:9200/webindex' -d '
    {  
      "settings":{  
        "index":{  
          "refresh_interval":"1s",
          "number_of_shards":"10",
          "number_of_replicas" : "0"
        }
      },
      "mappings":{  
        "my_web":{  
          "properties":{  
            "url":{  
              "type":"string",
              "index":"not_analyzed"
            },
            "method":{  
              "type":"string",
              "index":"not_analyzed"
            },
            "charSet":{  
              "type":"string",
              "index":"not_analyzed"
            },
            "mimeType":{  
              "type":"string",
              "index":"not_analyzed"
            }
          }
        }
      }
    }'

Feel free to add any properties other than the above if you need them.

### Register Crawl Config Data

A crawling configuration is created by registering a document to .river\_web index as below.
This example crawls sites of http://www.codelibs.org/ and http://fess.codelibs.org/.

    $ curl -XPUT 'localhost:9200/.river_web/config/my_web' -d '{
        "index" : "webindex",
        "type" : "my_web",
        "urls" : ["http://www.codelibs.org/", "http://fess.codelibs.org/"],
        "include_urls" : ["http://www.codelibs.org/.*", "http://fess.codelibs.org/.*"],
        "max_depth" : 3,
        "max_access_count" : 100,
        "num_of_thread" : 5,
        "interval" : 1000,
        "target" : [
          {
            "pattern" : {
              "url" : "http://www.codelibs.org/.*",
              "mimeType" : "text/html"
            },
            "properties" : {
              "title" : {
                "text" : "title"
              },
              "body" : {
                "text" : "body"
              },
              "bodyAsHtml" : {
                "html" : "body"
              },
              "projects" : {
                "text" : "ul.nav-list li a",
                "isArray" : true
              }
            }
          },
          {
            "pattern" : {
              "url" : "http://fess.codelibs.org/.*",
              "mimeType" : "text/html"
            },
            "properties" : {
              "title" : {
                "text" : "title"
              },
              "body" : {
                "text" : "body",
                "trimSpaces" : true
              },
              "menus" : {
                "text" : "ul.nav-list li a",
                "isArray" : true
              }
            }
          }
        ]
    }'

The configuration is:

| Property                      | Type    | Description                                     |
|:------------------------------|:-------:|:------------------------------------------------|
| index                         | string  | Stored index name.                              |
| type                          | string  | Stored type name.                               |
| urls                          | array   | Start point of URL for crawling.                |
| include\_urls                 | array   | White list of URL for crawling.                 |
| exclude\_urls                 | array   | Black list of URL for crawling.                 |
| max\_depth                    | int     | Depth of crawling documents.                    |
| max\_access\_count            | int     | The number of crawling documents.               |
| num\_of\_thread               | int     | The number of crawler threads.                  |
| interval                      | int     | Interval time (ms) to crawl documents.          |
| incremental                   | boolean | Incremental crawling.                           |
| overwrite                     | boolean | Delete documents of old duplicated url.         |
| user\_agent                   | string  | User-agent name when crawling.                  |
| robots\_txt                   | boolean | If you want to ignore robots.txt, false.        |
| authentications               | object  | Specify BASIC/DIGEST/NTLM authentication info.  |
| target.urlPattern             | string  | URL pattern to extract contents by CSS Query.   |
| target.properties.name        | string  | "name" is used as a property name in the index. |
| target.properties.name.text   | string  | CSS Query for the property value.               |
| target.properties.name.html   | string  | CSS Query for the property value.               |
| target.properties.name.script | string  | Rewrite the property value by Script(Groovy).   |

### Start Crawler

    ./bin/riverweb --config-id [config doc id] --cluster-name [Elasticsearch Cluster Name] --cleanup

For example,

    ./bin/riverweb --config-id my_web --cluster-name elasticsearch --cleanup

### Unregister Crawl Config Data

If you want to stop the crawler, kill the crawler process and then delete the config document as below:

    $ curl -XDELETE 'localhost:9200/.river_web/config/my_web'

## Examples

### Full Text Search for Your site (ex. http://fess.codelibs.org/)

    $ curl -XPUT 'localhost:9200/.river_web/fess/fess_site' -d '{
        "index" : "webindex",
        "type" : "fess_site",
        "urls" : ["http://fess.codelibs.org/"],
        "include_urls" : ["http://fess.codelibs.org/.*"],
        "max_depth" : 3,
        "max_access_count" : 1000,
        "num_of_thread" : 5,
        "interval" : 1000,
        "target" : [
          {
            "pattern" : {
                "url" : "http://fess.codelibs.org/.*",
                "mimeType" : "text/html"
            },
            "properties" : {
                "title" : {
                    "text" : "title"
                },
                "body" : {
                    "text" : "body",
                    "trimSpaces" : true
                }
            }
          }
        ]
    }'


### Aggregate a title/content from news.yahoo.com

    $ curl -XPUT 'localhost:9200/.river_web/config/yahoo_site' -d '{
        "index" : "webindex",
        "type" : "my_web",
        "urls" : ["http://news.yahoo.com/"],
        "include_urls" : ["http://news.yahoo.com/.*"],
        "max_depth" : 1,
        "max_access_count" : 10,
        "num_of_thread" : 3,
        "interval" : 3000,
        "user_agent" : "Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko",
        "target" : [
          {
            "pattern" : {
              "url" : "http://news.yahoo.com/video/.*html",
              "mimeType" : "text/html"
            },
            "properties" : {
              "title" : {
                "text" : "title"
              }
            }
          },
          {
            "pattern" : {
              "url" : "http://news.yahoo.com/.*html",
              "mimeType" : "text/html"
            },
            "properties" : {
              "title" : {
                "text" : "h1.headline"
              },
              "content" : {
                "text" : "section#mediacontentstory p"
              }
            }
          }
        ]
    }'

(if news.yahoo.com is updated, the above example needs to be updated.)

## Others

### BASIC/DIGEST/NTLM authentication

River Web supports BASIC/DIGEST/NTLM authentication.
Set authentications object.

    ...
    "num_of_thread" : 5,
    "interval" : 1000,
    "authentications":[
      {
        "scope": {
          "scheme":"BASIC"
        },
        "credentials": {
          "username":"testuser",
          "password":"secret"
        }
      }],
    "target" : [
    ...

The configuration is:

| Property                                | Type    | Description                                     |
|:----------------------------------------|:-------:|:------------------------------------------------|
| authentications.scope.scheme            | string  | BASIC, DIGEST or NTLM                           |
| authentications.scope.host              | string  | (Optional)Target hostname.                      |
| authentications.scope.port              | int     | (Optional)Port number.                          |
| authentications.scope.realm             | string  | (Optional)Realm name.                           |
| authentications.credentials.username    | string  | Username.                                       |
| authentications.credentials.password    | string  | Password.                                       |
| authentications.credentials.workstation | string  | (Optional)Workstation for NTLM.                 |
| authentications.credentials.domain      | string  | (Optional)Domain for NTLM.                      |

For example, if you want to use an user in ActiveDirectory, the configuration is below:

    "authentications":[
      {
        "scope": {
          "scheme":"NTLM"
        },
        "credentials": {
          "domain":"your.ad.domain",
          "username":"taro",
          "password":"himitsu"
        }
      }],


### Use attachment type

River Web supports [attachment type](https://github.com/elasticsearch/elasticsearch-mapper-attachments).
For example, create a mapping with attachment type:

    curl -XPUT "localhost:9200/web/test/_mapping?pretty" -d '{
      "test" : {
        "properties" : {
    ...
          "my_attachment" : {
              "type" : "attachment",
              "fields" : {
                "file" : { "index" : "no" },
                "title" : { "store" : "yes" },
                "date" : { "store" : "yes" },
                "author" : { "store" : "yes" },
                "keywords" : { "store" : "yes" },
                "content_type" : { "store" : "yes" },
                "content_length" : { "store" : "yes" }
              }
          }
    ...

and then start your river. In "properties" object, when a value of "type" is "attachment", the crawled url is stored as base64-encoded data.

    curl -XPUT localhost:9200/.river_web/config/2 -d '{
          "index" : "web",
          "type" : "data",
          "urls" : "http://...",
    ...
          "target" : [
    ...
            {
              "settings" : {
                "html" : false
              },
              "pattern" : {
                "url" : "http://.../.*"
              },
              "properties" : {
                "my_attachment" : {
                  "type" : "attachment"
                }
              }
            }
          ]
    ...

### Use Multibyte Characters

An example in Japanese environment is below.
First, put some configuration file into conf directory of Elasticsearch.

    $ cd $ES_HOME/conf    # ex. /etc/elasticsearch if using rpm package
    $ sudo wget https://raw.github.com/codelibs/fess-server/master/src/tomcat/solr/core1/conf/mapping_ja.txt
    $ sudo wget http://svn.apache.org/repos/asf/lucene/dev/trunk/solr/example/solr/collection1/conf/lang/stopwords_ja.txt 

and then create "webindex" index with analyzers for Japanese.
(If you want to use uni-gram, remove cjk\_bigram in filter)

    $ curl -XPUT "localhost:9200/webindex" -d '
    {
      "settings" : {
        "analysis" : {
          "analyzer" : {
            "default" : {
              "type" : "custom",
              "char_filter" : ["mappingJa"],
              "tokenizer" : "standard",
              "filter" : ["word_delimiter", "lowercase", "cjk_width", "cjk_bigram"]
            }
          },
          "char_filter" : {
            "mappingJa": {
              "type" : "mapping",
              "mappings_path" : "mapping_ja.txt"
            }
          },
          "filter" : {
            "stopJa" : {
              "type" : "stop",
              "stopwords_path" : "stopwords_ja.txt"
            }
          }
        }
      }
    }'

### Rewrite a property value by Script

River Web allows you to rewrite crawled data by Java's ScriptEngine.
"javascript" is available.
In "properties" object, put "script" value to a property you want to rewrite.

    ...
            "properties" : {
    ...
              "flag" : {
                "text" : "body",
                "script" : "value.indexOf('Elasticsearch') > 0 ? 'yes' : 'no';"
              },

The above is, if a string value of body element in HTML contains "Elasticsearch", set "yes" to "flag" property.

### Use HTTP proxy

Put "proxy" property in "crawl" property.

    curl -XPUT 'localhost:9200/.river_web/config/my_web' -d '{
        "index" : "webindex",
        "type" : "my_web",
    ...
            "proxy" : {
              "host" : "proxy.server.com",
              "port" : 8080
            },

### Specify next crawled urls when crawling

To set "isChildUrl" property to true, the property values is used as next crawled urls.

    ...
        "target" : [
          {
    ...
            "properties" : {
              "childUrl" : {
                "value" : ["http://fess.codelibs.org/","http://fess.codelibs.org/ja/"],
                "isArray" : true,
                "isChildUrl" : true
              },

### Intercept start/execute/finish/close actions

You can insert your script to Executing Crawler(execute)/Finished Crawler(finish).
To insert scripts, put "script" property as below:

    curl -XPUT 'localhost:9200/.river_web/config/my_web' -d '{
        "script":{
          "execute":"your script...",
          "finish":"your script...",
        },
        ...

## FAQ

### What does "No scraping rule." mean?

In a river setting, "url" is starting urls to crawl a site, "include_urls" filters urls whether are crawled or not, and "target.pattern.url" is a rule to store extracted web data.
If a crawling url does not match "target.pattern.url", you would see the message.
Therefore, it means the crawled url does not have an extraction rule.

### How to extract an attribute of meta tag

For example, if you want to grab a content of description's meta tag, the configuration is below:

    ...
    "target" : [
    ...
      "properties" : {
    ...
        "meta" : {
          "attr" : "meta[name=description]",
          "args" : [ "content" ]
        },

### Incremental crawling dose not work?

"url" field needs to be "not\_analyzed" in a mapping of your stored index.
See [Create Index To Store Crawl Data](https://github.com/codelibs/elasticsearch-river-web#create-index-to-store-crawl-data "Create Index To Store Crawl Data").


### Where is crawled data stored?

crawled data are stored to ".s2robot" index during cralwing, data extracted from them are stored to your index specified by a river setting, and then data in "robot" index are removed when the crawler is finished.

## Powered By

* [Lasta Di](https://github.com/lastaflute/lasta-di "Lasta Di"): DI Container
* [Fess Crawler](https://github.com/codelibs/fess-crawler "Fess Crawler"): Web Crawler
