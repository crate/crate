package crate.elasticsearch.blob.v2;

import crate.elasticsearch.blob.BlobTransferTarget;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

public class BlobShardModule extends AbstractModule {

    private final Settings settings;

    @Inject
    public BlobShardModule(@IndexSettings Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        if (settings. getAsBoolean("index.blobs.enabled", false)){
            bind(BlobShard.class).asEagerSingleton();
        }
    }
}
