package crate.elasticsearch.action.searchinto;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Container class for inout specific informations.
 */
public class SearchIntoContext extends SearchContext {

    // currently we only support index targets
    private String targetType = "index";

    private List<InetSocketTransportAddress> targetNodes;


    public Map<String, String> outputNames() {
        return outputNames;
    }

    private final Map<String, String> outputNames = new HashMap<String,
            String>();

    public SearchIntoContext(long id, ShardSearchRequest request,
            SearchShardTarget shardTarget, Engine.Searcher engineSearcher,
            IndexService indexService, IndexShard indexShard,
            ScriptService scriptService, CacheRecycler cacheRecycler) {
        super(id, request, shardTarget, engineSearcher, indexService,
                indexShard, scriptService, cacheRecycler);
    }

    public String targetType() {
        // this is currently the only type supported
        return targetType;
    }

    public List<InetSocketTransportAddress> targetNodes() {
        if (targetNodes == null) {
            targetNodes = Lists.newArrayList();
        }
        return targetNodes;
    }

    public void emptyTargetNodes() {
        this.targetNodes = ImmutableList.of();
    }

}
