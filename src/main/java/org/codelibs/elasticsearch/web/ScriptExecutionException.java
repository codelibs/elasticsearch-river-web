package org.codelibs.elasticsearch.web;

public class ScriptExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ScriptExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

}
