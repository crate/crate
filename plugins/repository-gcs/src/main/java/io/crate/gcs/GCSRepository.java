package io.crate.gcs;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;


/**
 * Google Cloud Storage implementation of the BlobStoreRepository
 * <p>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code base_path}</dt><dd>Optional base path for all snapshot files.</dd>
 * </dl>
 */
public class GCSRepository extends BlobStoreRepository {

    static final Setting<String> BUCKET_SETTING =
        Setting.simpleString("bucket");

    static final Setting<String> BASE_PATH_SETTING =
        Setting.simpleString("base_path", Property.NodeScope);

    public GCSRepository(RepositoryMetadata metadata,
                         NamedXContentRegistry namedXContentRegistry,
                         ClusterService clusterService,
                         RecoverySettings recoverySettings) {
        super(metadata, namedXContentRegistry, clusterService,
            recoverySettings, buildBasePath(metadata));
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        return Strings.hasLength(basePath) ?
            new BlobPath().add(basePath) :
            BlobPath.cleanPath();
    }

    @Override
    protected BlobStore createBlobStore() {
        final String bucket = BUCKET_SETTING.get(metadata.settings());
        return new GCSBlobStore(bucket);
    }
}
