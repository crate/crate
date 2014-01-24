package org.cratedb.plugin;

import org.cratedb.module.SQLModule;
import org.cratedb.rest.action.RestSQLAction;
import org.cratedb.service.InformationSchemaService;
import org.cratedb.service.SQLService;
import org.cratedb.service.StatsService;
import org.cratedb.sql.facet.SQLFacetParser;
import org.elasticsearch.cluster.settings.ClusterDynamicSettingsModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.search.facet.FacetModule;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

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

        // do not map source on GetRequests
        // evaluated in elasticsearch ShardGetService.innerGet
        settingsBuilder.put("index.mapper.map_source", false);
        return settingsBuilder.build();
    }

    public String name() {
        return "sql";
    }

    public String description() {
        return "plugin that adds an /_sql endpoint to query crate with sql";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        if (!settings.getAsBoolean("node.client", false)) {
            Collection<Class<? extends LifecycleComponent>> services = newArrayList();
            services.add(SQLService.class);
            services.add(InformationSchemaService.class);
            services.add(StatsService.class);
            return services;
        }

        return super.services();
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
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

    public void onModule(ClusterDynamicSettingsModule clusterDynamicSettingsModule) {
        // add our dynamic cluster settings
        clusterDynamicSettingsModule.addDynamicSettings(SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX + "*");
    }

}