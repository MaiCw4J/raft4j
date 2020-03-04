package com.stephen.exception;


import org.slf4j.Logger;

public class PanicException extends RuntimeException {

    public PanicException(Logger log, String message, Throwable cause) {
        super(message);
        log.error(message, cause);
    }

    public PanicException(Logger log, String message, Object... val) {
        super(message);
        log.error(message, val);
    }

    public PanicException() {
        super();
    }

    public PanicException(String message) {
        super(message);
    }

    public PanicException(String message, Throwable cause) {
        super(message, cause);
    }

    public PanicException(Throwable cause) {
        super(cause);
    }

    protected PanicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
