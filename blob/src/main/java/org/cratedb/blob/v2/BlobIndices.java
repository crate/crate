package org.cratedb.blob.v2;

import org.cratedb.blob.BlobShardFuture;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

public class BlobIndices extends AbstractComponent {

    public static final String SETTING_BLOBS_ENABLED = "index.blobs.enabled";

    private final IndicesService indicesService;
    private final IndicesLifecycle indicesLifecycle;

    @Inject
    public BlobIndices(Settings settings, IndicesService indicesService,
                       IndicesLifecycle indicesLifecycle) {
        super(settings);
        this.indicesService = indicesService;
        this.indicesLifecycle = indicesLifecycle;
    }

    public boolean blobsEnabled(String index) {
        return indicesService.indexServiceSafe(index)
                .settingsService().getSettings().getAsBoolean(SETTING_BLOBS_ENABLED,
                        false);
    }

    public boolean blobsEnabled(ShardId shardId) {
        return blobsEnabled(shardId.getIndex());
    }

    public BlobShard blobShardSafe(ShardId shardId) {
        return blobShardSafe(shardId.getIndex(), shardId.id());
    }

    public BlobShard blobShard(String index, int shardId) {
        IndexService indexService = indicesService.indexService(index);
        if (indexService != null) {
            Injector injector = indexService.shardInjector(shardId);
            if (injector != null) {
                return injector.getInstance(BlobShard.class);
            }
        }

        return null;
    }

    public BlobShard blobShardSafe(String index, int shardId) {
        if (blobsEnabled(index)) {
            return indicesService.indexServiceSafe(index).shardInjectorSafe(shardId).getInstance(BlobShard.class);
        }
        throw new BlobsDisabledException(index);
    }

    public BlobIndex blobIndex(String index) {
        return indicesService.indexServiceSafe(index).injector().getInstance(BlobIndex.class);
    }

    public ShardId localShardId(String index, String digest) {
        return blobIndex(index).shardId(digest);
    }

    public BlobShard localBlobShard(String index, String digest) {
        return blobShardSafe(localShardId(index, digest));
    }

    public BlobShardFuture blobShardFuture(String index, int shardId) {
        return new BlobShardFuture(this, indicesLifecycle, index, shardId);

    }
}
