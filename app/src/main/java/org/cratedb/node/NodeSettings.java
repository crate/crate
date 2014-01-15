package org.cratedb.node;

import org.cratedb.Constants;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;

import java.net.URL;

public class NodeSettings {

    /**
     * Crate default settings
     *
     * @return
     */
    public static void applyDefaultSettings(ImmutableSettings.Builder settingsBuilder) {

        // set default ports so they will are set, even if not defined anywhere
        settingsBuilder.put("http.port", Constants.HTTP_PORT_RANGE);
        settingsBuilder.put("transport.tcp.port", Constants.TRANSPORT_PORT_RANGE);
        // TODO: use 4500-4600 for thrift?

        // read also from crate.yml by default if no other config path has been set
        // if there is also a elasticsearch.yml file this file will be read first and the settings in crate.yml
        // will overwrite them.
        Environment environment = new Environment(settingsBuilder.build());
        if (System.getProperty("es.config") == null && System.getProperty("elasticsearch.config") == null) {
            // no explicit config path set
            try {
                URL crateConfigUrl = environment.resolveConfig("crate.yml");
                settingsBuilder.loadFromUrl(crateConfigUrl);
            } catch (FailedToResolveConfigException e) {
                // ignore
            }
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