package io.crate.gcs;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import io.crate.analyze.repositories.TypeSettings;


/**
 * A plugin to add Google Cloud Storage as a repository.
 */
public class GCSRepositoryPlugin extends Plugin implements RepositoryPlugin {

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            GCSRepository.BUCKET_SETTING,
            GCSRepository.BASE_PATH_SETTING);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment environment, NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService, RecoverySettings recoverySettings) {
        return Collections.singletonMap(
            "gcs", new Repository.Factory() {
                @Override
                public TypeSettings settings() {
                    return new TypeSettings(
                        List.of(GCSRepository.BUCKET_SETTING), // Required
                        List.of(GCSRepository.BASE_PATH_SETTING)); // Optional
                }

                @Override
                public Repository create(RepositoryMetadata metadata) {
                    return new GCSRepository(
                        metadata, namedXContentRegistry,
                        clusterService, recoverySettings);
                }
            }
        );
    }
}
