package io.crate.operation;


public interface PageResultListener {

    void needMore(boolean needMore);
}
