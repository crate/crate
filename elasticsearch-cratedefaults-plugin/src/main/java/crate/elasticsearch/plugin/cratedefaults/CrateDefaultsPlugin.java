package crate.elasticsearch.plugin.cratedefaults;


import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.plugins.AbstractPlugin;

import java.net.URL;

/**
 * Crate Defaults Plugin sets some default crate settings:
 *
 * <ul>
 * <li>Default cluster name "crate"</li>
 * <li>Delete all indices is disabled</li>
 * <li>Apply additional crate settings from "crate.yml", "crate.json" or "crate.properties" files</li>
 * </ul>
 */
public class CrateDefaultsPlugin extends AbstractPlugin {

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder
                .put("cluster.name", "crate")
                .put("action.disable_delete_all_indices", true);

        Environment environment = new Environment(settingsBuilder.build());
        elasticSearchCustomSettings(environment);

        try {
            settingsBuilder.loadFromUrl(environment.resolveConfig("crate.yml"));
        } catch (FailedToResolveConfigException e) {
            // ignore
        } catch (NoClassDefFoundError e) {
            // ignore, no yaml
        }
        try {
            settingsBuilder.loadFromUrl(environment.resolveConfig("crate.json"));
        } catch (FailedToResolveConfigException e) {
            // ignore
        }
        try {
            settingsBuilder.loadFromUrl(environment.resolveConfig("crate.properties"));
        } catch (FailedToResolveConfigException e) {
            // ignore
        }

        return settingsBuilder.build();
    }

    @Override
    public String name() {
        return "cratedefaults";
    }

    @Override
    public String description() {
        return "Crate defaults Plugin";
    }

    private void elasticSearchCustomSettings(Environment environment) {
        URL url = null;
        try {
            url = environment.resolveConfig("elasticsearch.yml");
        } catch (FailedToResolveConfigException e) {
            // good
        } catch (NoClassDefFoundError e) {
            // good
        }
        try {
            url = environment.resolveConfig("elasticsearch.json");
        } catch (FailedToResolveConfigException e) {
            // good
        }
        try {
            url = environment.resolveConfig("elasticsearch.properties");
        } catch (FailedToResolveConfigException e) {
            // good
        }
        if (url != null) {
            throw new ElasticSearchException("Elasticsearch configuration found at '" + url.getPath() +
                                             "'. Only crate configuration files allowed.");
        }
    }
}
