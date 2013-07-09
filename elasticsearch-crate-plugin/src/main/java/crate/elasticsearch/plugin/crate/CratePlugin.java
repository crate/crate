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
import java.util.Arrays;
import java.util.List;

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

    private static final String pattern = "^(elasticsearch|es)\\..*$";

    private static final List<String> whitelist = Arrays.asList(
            "es.logger.prefix", "es.pidfile", "es.foreground", "es.max-open-files");


    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder
                .put("cluster.name", "crate")
                .put("action.disable_delete_all_indices", true);


        checkForElasticSearchSystemSettings();
        settingsBuilder.putProperties("crate.", System.getProperties());

        settingsBuilder.replacePropertyPlaceholders();

        Environment environment = new Environment(settingsBuilder.build());
        checkForElasticSearchCustomSettings(environment);

        boolean loadFromEnv = true;
        // if its default, then load it, but also load form env
        if (System.getProperty("crate.default.config") != null) {
            loadFromEnv = true;
            settingsBuilder.loadFromUrl(environment.resolveConfig(System.getProperty("crate.default.config")));
        }
        // if explicit, just load it and don't load from env
        if (System.getProperty("crate.config") != null) {
            loadFromEnv = false;
            settingsBuilder.loadFromUrl(environment.resolveConfig(System.getProperty("crate.config")));
        }
        if (loadFromEnv) {

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
     *
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

    /**
     * Raise an exception if any given system property starts with es or elasticsearch
     */
    private void checkForElasticSearchSystemSettings() {
        for (Object key : System.getProperties().keySet()) {
            if (whitelist.contains(key.toString())) {
                continue;
            }
            if (key.toString().matches(pattern)) {
                throw new ElasticSearchException("Elasticsearch system properties found: '" + key.toString() +
                        "'. Use prefix 'crate.' for system properties.");
            }
        }
    }
}
