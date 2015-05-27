package org.codelibs.elasticsearch.web;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.UUID;
import java.util.function.IntConsumer;

import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.index.query.QueryBuilders;

public class RiverWebTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        clusterName = "es-river-web-" + UUID.randomUUID().toString();
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
            }
        }).build(newConfigs().clusterName(clusterName).ramIndexStore().numOfNode(3));

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
            final String riverWebSource =
                    "{\"index\":\""
                            + index
                            + "\",\"url\":[\"http://www.codelibs.org/\",\"http://fess.codelibs.org/\"]"
                            + ",\"includeFilter\":[\"http://www.codelibs.org/.*\",\"http://fess.codelibs.org/.*\"]"
                            + ",\"excludeFilter\":[\".*\\\\.txt\",\".*\\\\.png\",\".*\\\\.gif\",\".*\\\\.js\",\".*\\\\.css\"]"
                            + ",\"maxDepth\":5,\"maxAccessCount\":100,\"numOfThread\":5,\"interval\":1000"
                            + ",\"target\":[{\"pattern\":{\"url\":\"http://www.codelibs.org/.*\",\"mimeType\":\"text/html\"}"
                            + ",\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\"},\"bodyAsHtml\":{\"html\":\"body\"},\"projects\":{\"text\":\"ul.nav-listlia\",\"isArray\":true}}}"
                            + ",{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"}"
                            + ",\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trimSpaces\":true},\"menus\":{\"text\":\"ul.nav-listlia\",\"isArray\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });

        assertTrue(runner.count(index, type).getCount() + " >= 100", runner.count(index, type).getCount() >= 100);

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
            final String riverWebSource =
                    "{\"index\":\""
                            + index
                            + "\",\"type\":\""
                            + type
                            + "\",\"url\":[\"http://fess.codelibs.org/\"],\"includeFilter\":[\"http://fess.codelibs.org/.*\"],\"maxDepth\":1,\"maxAccessCount\":1,\"numOfThread\":1,\"interval\":1000,\"overwrite\":true,\"target\":[{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"},\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trimSpaces\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getCount());
        SearchResponse response1 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getCount());
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
            final String riverWebSource =
                    "{\"index\":\""
                            + index
                            + "\",\"type\":\""
                            + type
                            + "\",\"url\":[\"http://fess.codelibs.org/\"],\"includeFilter\":[\"http://fess.codelibs.org/.*\"],\"maxDepth\":1,\"maxAccessCount\":1,\"numOfThread\":1,\"interval\":1000,\"incremental\":true,\"target\":[{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"},\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trimSpaces\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getCount());
        SearchResponse response1 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getCount());
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
            final String riverWebSource =
                    "{\"index\":\""
                            + index
                            + "\",\"type\":\""
                            + type
                            + "\",\"url\":[\"http://fess.codelibs.org/\"],\"includeFilter\":[\"http://fess.codelibs.org/.*\"],\"maxDepth\":1,\"maxAccessCount\":1,\"numOfThread\":1,\"interval\":1000,\"target\":[{\"pattern\":{\"url\":\"http://fess.codelibs.org/.*\",\"mimeType\":\"text/html\"},\"properties\":{\"title\":{\"text\":\"title\"},\"body\":{\"text\":\"body\",\"trimSpaces\":true}}}]}";
            final IndexResponse response = runner.insert(riverWebIndex, riverWebType, config, riverWebSource);
            if (!response.isCreated()) {
                fail();
            }
        }

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(1, runner.count(index, type).getCount());
        SearchResponse response1 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 1);

        RiverWeb.main(new String[] { "--config-id", config, "--es-port", runner.node().settings().get("transport.tcp.port"),
                "--cluster-name", clusterName, "--cleanup" });
        assertEquals(2, runner.count(index, type).getCount());
        SearchResponse response2 = runner.search(index, type, QueryBuilders.termQuery("url", "http://fess.codelibs.org/"), null, 0, 2);

        assertEquals(1, response1.getHits().getTotalHits());
        assertEquals(2, response2.getHits().getTotalHits());

        runner.ensureYellow();
    }
}
