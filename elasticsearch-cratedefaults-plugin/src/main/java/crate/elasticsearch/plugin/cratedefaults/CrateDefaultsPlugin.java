package crate.elasticsearch.plugin.cratedefaults;


import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.plugins.AbstractPlugin;

/**
 *
 */
public class CrateDefaultsPlugin extends AbstractPlugin {

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder
                .put("cluster.name", "crate")
                .put("action.disable_delete_all_indices", true);

        Environment environment = new Environment(settingsBuilder.build());

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
}
