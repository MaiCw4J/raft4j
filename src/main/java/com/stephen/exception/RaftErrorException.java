package com.stephen.exception;

import lombok.Getter;

public class RaftErrorException extends Exception {

    @Getter
    private RaftError error;

    public RaftErrorException(RaftError error) {
        super(error.getDescription());
        this.error = error;
    }

    public RaftErrorException(RaftError error, String msg) {
        super(msg);
        this.error = error;
    }

    public RaftErrorException(RaftError error, Object... msg) {
        super(String.format(error.getDescription(), msg));
        this.error = error;
    }

}
