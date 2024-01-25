package io.crate.gcs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;

/**
 * Google Cloud Storage implementation of the BlobStoreRepository
 * <p>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code endpoint}</dt><dd>Endpoint root URL (only used for testing)</dd>
 * <dt>{@code service_account_key}</dt><dd>JSON key file of a Service Account that has access to the specified bucket.</dd>
 * <dt>{@code bucket}</dt><dd>Bucket name</dd>
 * <dt>{@code base_path}</dt><dd>Base path (blob name prefix) in the bucket</dd>
 * </dl>
 */
public class GCSRepository extends BlobStoreRepository {

    static final Setting<String> ENDPOINT_SETTING =
        Setting.simpleString("endpoint", Property.NodeScope);

    static final Setting<String> PROJECT_ID_SETTING =
        Setting.simpleString("project_id");

    static final Setting<String> PRIVATE_KEY_ID_SETTING =
        Setting.simpleString("private_key_id");

    static final Setting<String> PRIVATE_KEY_SETTING =
        Setting.simpleString("private_key");

    static final Setting<String> CLIENT_EMAIL_SETTING =
        Setting.simpleString("client_email");

    static final Setting<String> CLIENT_ID_SETTING =
        Setting.simpleString("client_id");

    static final Setting<String> AUTH_URI_SETTING =
        Setting.simpleString("auth_uri");

    static final Setting<String> TOKEN_URI_SETTING =
        Setting.simpleString("token_uri");

    static final Setting<String> AUTH_PROVIDER_X509_CERT_URL =
        Setting.simpleString("auth_provider_x509_cert_url");

    static final Setting<String> CLIENT_X509_CERT_URL_SETTING =
        Setting.simpleString("client_x509_cert_url");

    static final Setting<String> UNIVERSE_DOMAIN_SETTING =
        Setting.simpleString("universe_domain");

    static final Setting<String> SERVICE_ACCOUNT_KEY_SETTING =
        Setting.simpleString("service_account_key");

    static final Setting<String> BUCKET_SETTING =
        Setting.simpleString("bucket", Property.NodeScope);

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
    protected BlobStore createBlobStore() throws java.io.IOException {
        final Settings settings = metadata.settings();
        final StorageOptions.Builder opts = StorageOptions.newBuilder();

        // A custom endpoint is only used for locally testing this plugin,
        // e.g. using https://github.com/fsouza/fake-gcs-server.
        if (ENDPOINT_SETTING.exists(settings)) {
            opts.setHost(ENDPOINT_SETTING.get(settings));
        }

        // Set service account credentials.
        final URI tokenServiceUrl;
        try {
            tokenServiceUrl = new URI(TOKEN_URI_SETTING.get(settings));
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getCause());
        }
        final ServiceAccountCredentials credentials = ServiceAccountCredentials.newBuilder()
            .setProjectId(PROJECT_ID_SETTING.get(settings))
            .setPrivateKeyId(PRIVATE_KEY_ID_SETTING.get(settings))
            .setPrivateKeyString(PRIVATE_KEY_SETTING.get(settings))
            .setClientEmail(CLIENT_EMAIL_SETTING.get(settings))
            .setClientId(CLIENT_ID_SETTING.get(settings))
            .setTokenServerUri(tokenServiceUrl).build();
        opts.setCredentials(credentials);

        // Get the specified bucket.
        final String bucketName = BUCKET_SETTING.get(s);
        final Storage storage = opts.build().getService();
        final Bucket bucket = storage.get(bucketName);
        return new GCSBlobStore(bucket);
    }
}
