package io.crate.operator.reference.sys;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class SysExpressionModule extends AbstractModule {

    @Override
    protected void configure() {
        MapBinder<ReferenceIdent, ReferenceImplementation> b = MapBinder
                .newMapBinder(binder(), ReferenceIdent.class, ReferenceImplementation.class);
        b.addBinding(NodeLoadExpression.INFO_LOAD.ident()).to(NodeLoadExpression.class).asEagerSingleton();
    }
}
