package crate.elasticsearch.plugin.facet;

import crate.elasticsearch.facet.distinct.DistinctDateHistogramFacetParser;
import crate.elasticsearch.facet.distinct.InternalDistinctDateHistogramFacet;
import crate.elasticsearch.facet.latest.LatestFacetParser;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

import java.util.Collection;


public class FacetPlugin extends AbstractPlugin {

    public FacetPlugin(Settings settings) {
    }

    @Override
    public String name() {
        return "time-facets";
    }

    @Override
    public String description() {
        return "Time-Facets Plugins";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(TimeFacetsModule.class);
        return modules;
    }

    public void onModule(FacetModule module) {
        module.addFacetProcessor(DistinctDateHistogramFacetParser.class);
        module.addFacetProcessor(LatestFacetParser.class);
    }
}
