package org.cratedb.sys;

import org.cratedb.sys.cluster.ClusterNameExpression;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;


/**
 * This module registers all system expressions
 */
public class SysExpressionModule extends AbstractModule {

    @Override
    protected void configure() {
        MapBinder<ScopedName, SysExpression> sysExpressionBinder =
                MapBinder.newMapBinder(binder(), ScopedName.class, SysExpression.class);

        sysExpressionBinder.addBinding(ClusterNameExpression.NAME).to(ClusterNameExpression.class).asEagerSingleton();

    }


}
