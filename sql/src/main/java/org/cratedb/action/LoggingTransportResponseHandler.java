package org.cratedb.action;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

public class LoggingTransportResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {

    private final ESLogger logger = Loggers.getLogger(getClass());

    public static final LoggingTransportResponseHandler INSTANCE_SAME = new LoggingTransportResponseHandler(ThreadPool.Names.SAME);

    private final String executor;

    public LoggingTransportResponseHandler(String executor) {
        this.executor = executor;
    }

    @Override
    public TransportResponse.Empty newInstance() {
        return TransportResponse.Empty.INSTANCE;
    }

    @Override
    public void handleResponse(TransportResponse.Empty response) {
        if (logger.isTraceEnabled()){
            logger.trace("received empty response {}", response);
        }
    }

    @Override
    public void handleException(TransportException exp) {
        logger.error("request failed", exp);
    }

    @Override
    public String executor() {
        return executor;
    }
}
