package crate.elasticsearch.plugin.crate;


import crate.elasticsearch.rest.action.admin.CrateFrontpageAction;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

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
public class CratePlugin extends AbstractPlugin {

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder
                .put("cluster.name", "crate")
                .put("action.disable_delete_all_indices", true);

        Environment environment = new Environment(settingsBuilder.build());
        checkForElasticSearchCustomSettings(environment);

        String[] ignorePrefixes = new String[]{"es.default.", "elasticsearch.default."};
        settingsBuilder.putProperties("elasticsearch.default.", System.getProperties())
                .putProperties("es.default.", System.getProperties())
                .putProperties("elasticsearch.", System.getProperties(), ignorePrefixes)
                .putProperties("es.", System.getProperties(), ignorePrefixes);

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
        return "crate";
    }

    @Override
    public String description() {
        return "Crate defaults Plugin";
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(CrateFrontpageAction.class);
    }

    /**
     * Raise an exception if there are elasticsearch config files (yaml, json or properties).
     * Only custom crate config files are allowed.
     * @param environment
     */
    private void checkForElasticSearchCustomSettings(Environment environment) {
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
                                             "'. Use crate configuration file.");
        }
    }
}
