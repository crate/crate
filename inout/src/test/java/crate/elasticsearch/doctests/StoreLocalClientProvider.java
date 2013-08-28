package crate.elasticsearch.doctests;

import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import com.github.tlrx.elasticsearch.test.provider.ClientProvider;
import com.github.tlrx.elasticsearch.test.provider.LocalClientProvider;

public class StoreLocalClientProvider extends LocalClientProvider implements ClientProvider {

    private Settings settings;

    public StoreLocalClientProvider() {
        super();
    }

    public StoreLocalClientProvider(Settings settings) {
        super(settings);
        this.settings = settings;
    }

    protected Settings buildNodeSettings() {
        // Build settings
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("node.name", "node-test-" + System.currentTimeMillis())
                .put("node.data", true)
                .put("cluster.name", "cluster-test-" + NetworkUtils.getLocalAddress().getHostName())
                .put("path.data", "./target/elasticsearch-test/data")
                .put("path.work", "./target/elasticsearch-test/work")
                .put("path.logs", "./target/elasticsearch-test/logs")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0")
                .put("cluster.routing.schedule", "50ms")
                .put("node.local", true);

        if (settings != null) {
            builder.put(settings);
        }

        return builder.build();
    }

}
