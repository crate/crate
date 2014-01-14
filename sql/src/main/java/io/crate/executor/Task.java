package io.crate.executor;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface Task<ResultType> {

    public void start();

    public List<ListenableFuture<ResultType>> result();

    public void upstreamResult(List<ListenableFuture<ResultType>> result);

}
