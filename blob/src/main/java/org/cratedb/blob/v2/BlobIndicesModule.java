package org.cratedb.blob.v2;

import org.cratedb.blob.BlobTransferTarget;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.indices.recovery.BlobRecoverySource;
import org.elasticsearch.indices.recovery.BlobRecoveryTarget;


public class BlobIndicesModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(BlobIndices.class).asEagerSingleton();
        bind(BlobRecoverySource.class).asEagerSingleton();
        bind(BlobRecoveryTarget.class).asEagerSingleton();
        bind(BlobTransferTarget.class).asEagerSingleton();
    }
}
