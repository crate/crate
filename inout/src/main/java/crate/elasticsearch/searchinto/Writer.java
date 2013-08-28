package crate.elasticsearch.searchinto;

import crate.elasticsearch.action.searchinto.SearchIntoContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Map;

public class Writer {

    private static final ESLogger logger = Loggers.getLogger(Writer.class);

    private final Map<String, WriterCollectorFactory> collectors;

    @Inject
    public Writer(Map<String, WriterCollectorFactory> collectors) {
        this.collectors = collectors;
    }


    public WriterResult execute(SearchIntoContext context) throws
            WriterException {
        logger.info("writing {}/{}", context.shardTarget().index(),
                context.shardTarget().getShardId());
        Query query = context.query();
        context.version(true);
        WriterCollector wc = collectors.get(context.targetType()).create(
                context);
        wc.open();
        try {
            context.searcher().search(query, wc);
        } catch (IOException e) {
            throw new WriterException(context, "Failed to write docs", e);
        }
        wc.close();
        WriterResult res = wc.getResult();
        logger.info("exported {} docs from {}/{}", res.getTotalWrites(),
                context.shardTarget().index(),
                context.shardTarget().getShardId());
        return res;


    }

}
