package org.cratedb.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;

public class NodeSettings {

    /**
     * Crate default settings
     *
     * @return
     */
    public static void applyDefaultSettings(ImmutableSettings.Builder settingsBuilder) {

        // read also from crate.yml by default if no other config path has been set
        // if there is also a elasticsearch.yml file this file will be read first and the settings in crate.yml
        // will overwrite them.
        Environment environment = new Environment(settingsBuilder.build());
        if (System.getProperty("es.config") == null && System.getProperty("elasticsearch.config") == null) {
            // no explicit config path set
            settingsBuilder.loadFromUrl(environment.resolveConfig("crate.yml"));
        }

        // Forbid DELETE on '/' aka. deleting all indices
        settingsBuilder.put("action.disable_delete_all_indices", true);

        // Set the default cluster name if not explicitly defined
        if (settingsBuilder.get(ClusterName.SETTING).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put("cluster.name", "crate");
        }

        int availableProcessors = Math.min(32, Runtime.getRuntime().availableProcessors());
        int halfProcMaxAt10 = Math.min(((availableProcessors + 1) / 2), 10);
        settingsBuilder.put("threadpool.generic.size", halfProcMaxAt10);
    }
}