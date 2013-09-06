package org.cratedb.blob.v2;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;

public class BlobIndexModule extends AbstractModule {

    private final Settings settings;

    public BlobIndexModule(@IndexSettings Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        if (settings.getAsBoolean("index.blobs.enabled", false)){
            bind(BlobIndex.class).asEagerSingleton();
        }
    }
}
