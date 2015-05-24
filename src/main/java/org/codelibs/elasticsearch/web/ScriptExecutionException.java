package org.codelibs.elasticsearch.web;

public class ScriptExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ScriptExecutionException(final String message) {
        super(message);
    }

    public ScriptExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
