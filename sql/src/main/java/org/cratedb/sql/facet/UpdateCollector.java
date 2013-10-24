package org.cratedb.sql.facet;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A Collector which generates elasticsearch update requests for every document it collects
 */
public class UpdateCollector extends FacetExecutor.Collector {

    private final SearchLookup lookup;
    private final TransportUpdateAction updateAction;
    private final Map<String, Object> updateDoc;
    private final ShardId shardId;
    private long rowCount;
    private Long requiredVersion;
    private AtomicReaderContext currentReaderContext;

    public long rowCount() {
        return rowCount;
    }

    class CollectorUpdateRequest extends UpdateRequest {

        CollectorUpdateRequest(ShardId shardId, Uid uid) {
            super(shardId.getIndex(), uid.type(), uid.id());
            this.shardId = shardId.id();
            retryOnConflict(ParsedStatement.UPDATE_RETRY_ON_CONFLICT);
            doc(updateDoc);
        }
    }

    public UpdateCollector(
            Map<String, Object> updateDoc,
            TransportUpdateAction updateAction,
            SearchContext context,
            Long requiredVersion) {
        this.shardId = context.indexShard().shardId();
        this.updateAction = updateAction;
        this.lookup = context.lookup();
        this.updateDoc = updateDoc;
        this.rowCount = 0;
        this.requiredVersion = requiredVersion;
    }

    @Override
    public void setScorer(Scorer scorer) {
        lookup.setScorer(scorer);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.currentReaderContext = context;
        lookup.setNextReader(context);
    }


    @Override
    public void postCollection() {
        // nothing to do here
    }

    @Override
    public void collect(int doc) throws IOException {
        lookup.setNextDocId(doc);
        Uid uid = Uid.createUid(((ScriptDocValues.Strings) lookup.doc().get("_uid")).getValue());
        if (requiredVersion != null) {
            Long currentVersion = UidField.loadVersion(
                    currentReaderContext,
                    new Term(UidFieldMapper.NAME, uid.toBytesRef())
            );

            if (!currentVersion.equals(requiredVersion)) {
                return;
            }
        }
        collect(uid);
    }

    private void collect(Uid uid) {
        UpdateRequest request = new CollectorUpdateRequest(shardId, uid);
        // Since we are sequential here, it should be ok to reuse the same request instance
        UpdateResponse response = updateAction.execute(request).actionGet();
        rowCount++;
    }
}
