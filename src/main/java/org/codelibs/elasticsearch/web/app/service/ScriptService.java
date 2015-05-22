package org.codelibs.elasticsearch.web.app.service;

import java.util.Map;

import javax.annotation.Resource;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.codelibs.core.io.FileUtil;
import org.codelibs.elasticsearch.web.ScriptExecutionException;
import org.codelibs.elasticsearch.web.client.EsClient;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ScriptService {
    @Resource
    protected EsClient esClient;

    public Object execute(final String lang, final String script, final ScriptType scriptType, final Map<String, Object> localVars) {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName(lang);

        for (Map.Entry<String, Object> entry : localVars.entrySet()) {
            engine.put(entry.getKey(), entry.getValue());
        }
        try {
            return engine.eval(getScriptContent(lang, script, scriptType));
        } catch (ScriptException e) {
            throw new ScriptExecutionException("lang: " + lang + ", script: " + script + ", type: " + scriptType, e);
        }
    }

    private String getScriptContent(String lang, String script, ScriptType scriptType) {
        switch (scriptType) {
        case INLINE:
            return script;
        case FILE:
            return FileUtil.readText(script);
        case INDEXED:
            Map<String, Object> source = esClient.prepareGet(".scripts", lang, script).execute().actionGet().getSource();
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
