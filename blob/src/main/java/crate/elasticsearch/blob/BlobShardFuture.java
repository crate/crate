package crate.elasticsearch.blob;

import crate.elasticsearch.blob.v2.BlobIndices;
import crate.elasticsearch.blob.v2.BlobShard;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;

public class BlobShardFuture extends BaseFuture<BlobShard> {

    public BlobShardFuture(final BlobIndices blobIndices, final IndicesLifecycle indicesLifecycle,
                           final String index, final int shardId)
    {
        BlobShard blobShard = blobIndices.blobShard(index, shardId);
        if (blobShard != null) {
            set(blobShard);
            return;
        }

        IndicesLifecycle.Listener listener = new IndicesLifecycle.Listener() {

            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                super.afterIndexShardCreated(indexShard);
                if (indexShard.shardId().index().getName().equals(index)) {
                    set(blobIndices.blobShardSafe(index, shardId));
                    indicesLifecycle.removeListener(this);
                }
            }
        };
        indicesLifecycle.addListener(listener);

        /**
         * prevent race condition:
         * if the index is created before the listener is added
         * but after the first blobShard() call
         */
        if (!isDone()) {
            blobShard = blobIndices.blobShard(index, shardId);
            if (blobShard != null) {
                indicesLifecycle.removeListener(listener);
                set(blobShard);
            }
        }
    }
}
