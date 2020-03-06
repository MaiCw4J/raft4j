package com.stephen;

import com.stephen.exception.RaftErrorException;

@FunctionalInterface
public interface CheckExceptionFunction<T> {

    /**
     * Gets a result.
     *
     * @return a result
     */
    T get() throws RaftErrorException;

}
