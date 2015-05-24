package org.codelibs.elasticsearch.web.app.service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import javax.annotation.Resource;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.codelibs.core.io.FileUtil;
import org.codelibs.elasticsearch.web.ScriptExecutionException;
import org.codelibs.elasticsearch.web.client.EsClient;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ScriptService {
    protected static final String SCRIPT_INDEX = ".scripts";

    @Resource
    protected EsClient esClient;

    public Object execute(final String lang, final String script, final ScriptType scriptType, final Map<String, Object> localVars) {
        final ScriptEngineManager manager = new ScriptEngineManager();
        final ScriptEngine engine = manager.getEngineByName(lang);

        for (final Map.Entry<String, Object> entry : localVars.entrySet()) {
            engine.put(entry.getKey(), entry.getValue());
        }
        try {
            return engine.eval(getScriptContent(lang, script, scriptType));
        } catch (final ScriptException e) {
            throw new ScriptExecutionException("lang: " + lang + ", script: " + script + ", type: " + scriptType, e);
        }
    }

    private String getScriptContent(final String lang, final String script, final ScriptType scriptType) {
        switch (scriptType) {
        case INLINE:
            return script;
        case FILE:
            if (Files.exists(Paths.get(script))) {
                return FileUtil.readText(new File(script));
            } else {
                return FileUtil.readText(script);
            }
        case INDEXED:
            final GetResponse response = esClient.prepareGet(SCRIPT_INDEX, lang, script).execute().actionGet();
            if (!response.isExists()) {
                throw new ScriptExecutionException("/" + SCRIPT_INDEX + "/" + lang + "/" + script + " does not exist.");
            }
            final Map<String, Object> source = response.getSource();
            if (source != null) {
                return (String) source.get("script");
            }
            break;
        default:
            break;
        }
        return null;
    }
}
