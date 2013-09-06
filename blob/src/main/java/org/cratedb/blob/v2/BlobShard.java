package org.cratedb.blob.v2;

import org.cratedb.blob.BlobContainer;
import org.cratedb.blob.BlobTransferTarget;
import org.cratedb.blob.stats.BlobStats;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;

import java.io.File;

public class BlobShard extends AbstractIndexShardComponent {

    private final BlobContainer blobContainer;
    private final IndexShard indexShard;

    @Inject
    protected BlobShard(ShardId shardId, Settings indexSettings,
            BlobTransferTarget transferTarget,
            NodeEnvironment nodeEnvironment,
            IndexShard indexShard) {
        super(shardId, indexSettings);
        this.indexShard = indexShard;
        File blobDir = new File(nodeEnvironment.shardLocations(shardId)[0], "blobs");
        logger.info("creating BlobContainer at {}", blobDir);
        this.blobContainer = new BlobContainer(blobDir);
    }

    public byte[][] currentDigests(byte prefix) {
        return blobContainer.cleanAndReturnDigests(prefix);
    }

    public boolean delete(String digest) {
        return blobContainer.getFile(digest).delete();
    }

    public BlobContainer blobContainer() {
        return blobContainer;
    }

    public ShardRouting shardRouting() {
        return indexShard.routingEntry();
    }

    public BlobStats blobStats() {
        final BlobStats stats = new BlobStats();

        stats.location(blobContainer().getBaseDirectory().getAbsolutePath());
        stats.availableSpace(blobContainer().getBaseDirectory().getFreeSpace());
        blobContainer().walkFiles(null, new BlobContainer.FileVisitor() {
            @Override
            public boolean visit(File file) {
                stats.totalUsage(stats.totalUsage() + file.length());
                stats.count(stats.count() + 1);
                return true;
            }
        });

        return stats;
    }
}
