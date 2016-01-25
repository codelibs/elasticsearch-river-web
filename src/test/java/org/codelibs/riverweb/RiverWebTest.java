package org.codelibs.riverweb;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.UUID;
import java.util.function.IntConsumer;

import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.riverweb.RiverWeb;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.QueryBuilders;

public class RiverWebTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private int numOfNode = 2;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        clusterName = "es-river-web-" + UUID.randomUUID().toString();
        runner = new ElasticsearchClusterRunner();
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.put("index.number_of_shards", 3);
                settingsBuilder.put("index.number_of_replicas", 0);
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
                settingsBuilder.put("index.unassigned.node_left.delayed_timeout", "0");
                settingsBuilder.put("network.host", "0");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(numOfNode));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_basic() throws Exception {

        RiverWeb.exitMethod = new IntConsumer() {
            @Override
            public void accept(final int value) {
                if (value != 0) {
                    fail();
                }
            }
        };

        final String index = "webindex";
        final String type = "my_web";
        final String riverWebIndex = ".river_web";
        final String riverWebType = "config";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final String mappingSource =
                "{\"my_web\":{\"dynamic_templates\":[{\"url\":{\"match\":\"url\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"method\":{\"match\":\"method\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"charSet\":{\"match\":\"charSet\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"mimeType\":{\"match\":\"mimeType\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}}]}}";
        runner.createMapping(index, type, mappingSource);

        if (!runner.indexExists(index)) {
            fail();
        }

        String config = type;
        {
            final String riverWebSource = "{\"index\":\"" + index
                    + "\",\"urls\":[\"http://www.codelibs.org/\",\"http://fess.codelibs.org/\"]"
                    + ",\"include_urls\":[\"http://www.codelibs.org/.*\",\"http://fess.codelibs.org/.*\"]"
                    + ",\"exclude_urls\":[\".*\\\\.txt\",\".*\\\\.png\",\".*\\\\.gif\",\".*\\\\.js\",\".*\\\\.css\"]"
                    + ",\"max_depth\":5,\"max_access_count\":100,\"num_of_thread\":5,\"interval\":1000"
                    + ",\"target\":[{\"pattern\":{\"url\":\"http://www.codelibs.org/.*\",\"mimeType\":\"text/html\"}"
                    + ",\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\"},\"bodyAsHtml\":{\"html\":\"body\"},\"projects\":{\"text\":\"ul.nav-listlia\",\"is_array\":true}}}"
                    + ",{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"}"
                    + ",\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trim_spaces\":true},\"menus\":{\"text\":\"ul.nav-listlia\",\"is_array\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });

        assertTrue(runner.count(index, type).getHits().getTotalHits() + " >= 100",
                runner.count(index, type).getHits().getTotalHits() >= 100);

        runner.ensureYellow();
    }

    public void test_overwrite() throws Exception {

        RiverWeb.exitMethod = new IntConsumer() {
            @Override
            public void accept(final int value) {
                if (value != 0) {
                    fail();
                }
            }
        };

        final String index = "webindex";
        final String type = "my_web";
        final String riverWebIndex = ".river_web";
        final String riverWebType = "config";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final String mappingSource =
                "{\"my_web\":{\"dynamic_templates\":[{\"url\":{\"match\":\"url\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"method\":{\"match\":\"method\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"charSet\":{\"match\":\"charSet\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"mimeType\":{\"match\":\"mimeType\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}}]}}";
        runner.createMapping(index, type, mappingSource);

        if (!runner.indexExists(index)) {
            fail();
        }

        String config = type;
        {
            final String riverWebSource = "{\"index\":\"" + index + "\",\"type\":\"" + type
                    + "\",\"urls\":[\"http://fess.codelibs.org/\"],\"include_urls\":[\"http://fess.codelibs.org/.*\"],\"max_depth\":1,\"max_access_count\":1,\"num_of_thread\":1,\"interval\":1000,\"overwrite\":true,\"target\":[{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"},\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trim_spaces\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getHits().getTotalHits());
        SearchResponse response1 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getHits().getTotalHits());
        SearchResponse response2 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        assertFalse(response1.getHits().getHits()[0].getSource().get("@timestamp")
                .equals(response2.getHits().getHits()[0].getSource().get("@timestamp")));

        runner.ensureYellow();
    }

    public void test_incremental() throws Exception {

        RiverWeb.exitMethod = new IntConsumer() {
            @Override
            public void accept(final int value) {
                if (value != 0) {
                    fail();
                }
            }
        };

        final String index = "webindex";
        final String type = "my_web";
        final String riverWebIndex = ".river_web";
        final String riverWebType = "config";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final String mappingSource =
                "{\"my_web\":{\"dynamic_templates\":[{\"url\":{\"match\":\"url\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"method\":{\"match\":\"method\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"charSet\":{\"match\":\"charSet\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"mimeType\":{\"match\":\"mimeType\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}}]}}";
        runner.createMapping(index, type, mappingSource);

        if (!runner.indexExists(index)) {
            fail();
        }

        String config = type;
        {
            final String riverWebSource = "{\"index\":\"" + index + "\",\"type\":\"" + type
                    + "\",\"urls\":[\"http://fess.codelibs.org/\"],\"include_urls\":[\"http://fess.codelibs.org/.*\"],\"max_depth\":1,\"max_access_count\":1,\"num_of_thread\":1,\"interval\":1000,\"incremental\":true,\"target\":[{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"},\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trim_spaces\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getHits().getTotalHits());
        SearchResponse response1 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getHits().getTotalHits());
        SearchResponse response2 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        assertEquals(response1.getHits().getHits()[0].getSource().get("@timestamp"),
                response2.getHits().getHits()[0].getSource().get("@timestamp"));

        runner.ensureYellow();
    }

    public void test_default() throws Exception {

        RiverWeb.exitMethod = new IntConsumer() {
            @Override
            public void accept(final int value) {
                if (value != 0) {
                    fail();
                }
            }
        };

        final String index = "webindex";
        final String type = "my_web";
        final String riverWebIndex = ".river_web";
        final String riverWebType = "config";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final String mappingSource =
                "{\"my_web\":{\"dynamic_templates\":[{\"url\":{\"match\":\"url\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"method\":{\"match\":\"method\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"charSet\":{\"match\":\"charSet\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}},{\"mimeType\":{\"match\":\"mimeType\",\"mapping\":{\"type\":\"string\",\"store\":\"yes\",\"index\":\"not_analyzed\"}}}]}}";
        runner.createMapping(index, type, mappingSource);

        if (!runner.indexExists(index)) {
            fail();
        }

        String config = type;
        {
            final String riverWebSource = "{\"index\":\"" + index + "\",\"type\":\"" + type
                    + "\",\"urls\":[\"http://fess.codelibs.org/\"],\"include_urls\":[\"http://fess.codelibs.org/.*\"],\"max_depth\":1,\"max_access_count\":1,\"num_of_thread\":1,\"interval\":1000,\"target\":[{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"},\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trim_spaces\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getHits().getTotalHits());
        SearchResponse response1 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        RiverWeb.main(new String[] { "--config-id", config, "--es-hosts", "localhost:" + runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(2, runner.count(index, type).getHits().getTotalHits());
        SearchResponse response2 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 2);

        assertEquals(1, response1.getHits().getTotalHits());
        assertEquals(2, response2.getHits().getTotalHits());

        runner.ensureYellow();
    }
}
