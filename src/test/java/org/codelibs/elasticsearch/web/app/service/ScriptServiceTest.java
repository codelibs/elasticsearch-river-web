package org.codelibs.elasticsearch.web.app.service;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Resource;

import org.codelibs.core.io.FileUtil;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.web.client.EsClient;
import org.dbflute.utflute.lastadi.ContainerTestCase;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ScriptServiceTest extends ContainerTestCase {
    @Resource
    protected ScriptService scriptService;

    @Resource
    protected EsClient esClient;

    public void test_javascript_inline() throws Exception {
        String lang = "javascript";
        String script = "'test';";
        ScriptType scriptType = ScriptType.INLINE;
        Map<String, Object> localVars = new HashMap<>();

        assertEquals("test", scriptService.execute(lang, script, scriptType, localVars));

        script = "print('test');";
        assertNull(scriptService.execute(lang, script, scriptType, localVars));

        localVars.put("testVar", "aaa");
        script = "testVar;";
        assertEquals("aaa", scriptService.execute(lang, script, scriptType, localVars));
    }

    public void test_javascript_file() throws Exception {
        File tempFile = File.createTempFile("temp", ".txt");
        tempFile.deleteOnExit();
        FileUtil.writeBytes(tempFile.getAbsolutePath(), "'test';".getBytes());
        String lang = "javascript";
        String script = tempFile.getAbsolutePath();
        ScriptType scriptType = ScriptType.FILE;
        Map<String, Object> localVars = new HashMap<>();

        assertEquals("test", scriptService.execute(lang, script, scriptType, localVars));

        FileUtil.writeBytes(tempFile.getAbsolutePath(), "print('test');".getBytes());
        assertNull(scriptService.execute(lang, script, scriptType, localVars));

        localVars.put("testVar", "aaa");
        FileUtil.writeBytes(tempFile.getAbsolutePath(), "testVar;".getBytes());
        assertEquals("aaa", scriptService.execute(lang, script, scriptType, localVars));
    }

    public void test_javascript_indexed() throws Exception {
        // create runner instance
        ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();
        String clusterName = "es-river-web-" + UUID.randomUUID().toString();
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
            }
        }).build(newConfigs().clusterName(clusterName).ramIndexStore().numOfNode(1));

        // wait for yellow status
        runner.ensureYellow();

        try {
            esClient.connect(clusterName, "localhost", Integer.parseInt(runner.node().settings().get("transport.tcp.port")));

            Map<String, Object> localVars = new HashMap<>();

            String lang = "javascript";
            ScriptType scriptType = ScriptType.INDEXED;
            String script = "script1";
            runner.insert(ScriptService.SCRIPT_INDEX, lang, script, "{\"script\":\"'test';\"}");

            assertEquals("test", scriptService.execute(lang, script, scriptType, localVars));

            script = "script2";
            runner.insert(ScriptService.SCRIPT_INDEX, lang, script, "{\"script\":\"print('test');\"}");
            assertNull(scriptService.execute(lang, script, scriptType, localVars));

            localVars.put("testVar", "aaa");
            script = "script3";
            runner.insert(ScriptService.SCRIPT_INDEX, lang, script, "{\"script\":\"testVar;\"}");
            assertEquals("aaa", scriptService.execute(lang, script, scriptType, localVars));
        } finally {
            // close runner
            runner.close();
            // delete all files
            runner.clean();
        }
    }
}
