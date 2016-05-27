package org.codelibs.riverweb.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.riverweb.WebRiverConstants;
import org.codelibs.riverweb.app.service.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.lastaflute.di.core.SingletonLaContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScriptUtils {
    public static final Logger logger = LoggerFactory.getLogger(ScriptUtils.class);

    private ScriptUtils() {
        // nothing
    }

    public static Object execute(final Map<String, Object> scriptSettings, final String target, final Consumer<Map<String, Object>> vars) {
        final String script = SettingsUtils.get(scriptSettings, target);
        final String lang = SettingsUtils.get(scriptSettings, "lang", WebRiverConstants.DEFAULT_SCRIPT_LANG);
        final String scriptTypeValue = SettingsUtils.get(scriptSettings, "script_type", "inline");
        ScriptType scriptType;
        if (ScriptType.FILE.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.FILE;
        } else if (ScriptType.INDEXED.toString().equalsIgnoreCase(scriptTypeValue)) {
            scriptType = ScriptType.INDEXED;
        } else {
            scriptType = ScriptType.INLINE;
        }
        if (StringUtil.isNotBlank(script)) {
            final Map<String, Object> localVars = new HashMap<String, Object>();
            vars.accept(localVars);
            try {
                final ScriptService scriptService = SingletonLaContainer.getComponent(ScriptService.class);
                final Object result = scriptService.execute(lang, script, scriptType, localVars);
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] \"{}\" => {}", target, script, result);
                }
                return result;
            } catch (final Exception e) {
                logger.warn("Failed to execute script: " + script, e);
            }
        }
        return null;
    }

}
