package org.cratedb.plugin;

import com.google.common.collect.Lists;
import org.cratedb.module.SQLModule;
import org.cratedb.rest.action.RestSQLAction;
import org.cratedb.sql.facet.SQLFacetParser;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.search.facet.FacetModule;

import java.util.Collection;

public class SQLPlugin extends AbstractPlugin {

    private final Settings settings;

    public SQLPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Settings additionalSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

        // Set default analyzer
        settingsBuilder.put("index.analysis.analyzer.default.type", "keyword");

        return settingsBuilder.build();
    }

    public String name() {
        return "sql";
    }

    public String description() {
        return "plugin that adds an /_sql endpoint to query crate with sql";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        if (!settings.getAsBoolean("node.client", false)) {
            modules.add(SQLModule.class);
        }
        return modules;
    }

    public void onModule(RestModule restModule) {
        restModule.addRestAction(RestSQLAction.class);
    }

    public void onModule(FacetModule facetModule) {
        facetModule.addFacetProcessor(SQLFacetParser.class);
    }

}