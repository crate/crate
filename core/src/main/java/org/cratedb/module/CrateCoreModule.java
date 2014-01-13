package org.cratedb.module;

import org.cratedb.ClusterIdService;
import org.cratedb.core.futures.GenericBaseFuture;
import org.cratedb.rest.CrateRestMainAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.action.main.RestMainAction;

import java.util.concurrent.TimeUnit;

public class CrateCoreModule extends AbstractModule {

    final ESLogger logger = Loggers.getLogger(getClass());

    @Override
    protected void configure() {
        /**
         * This is a rather hacky method to overwrite the handler for "/"
         * The ES plugins are loaded before the core ES components. That means that the registration for
         * "/" in {@link CrateRestMainAction} is overwritten once {@link RestMainAction} is instantiated.
         *
         * By using a listener that is called after {@link RestMainAction} is instantiated we can call
         * {@link org.cratedb.rest.CrateRestMainAction#registerHandler()}  and overwrite it "back".
         */

        // the crateListener is used to retrieve the CrateRestMainAction instance.
        // otherwise there is no way to retrieve it at this point.
        CrateRestMainActionListener crateListener = new CrateRestMainActionListener();
        bindListener(
            new SubclassOfMatcher(CrateRestMainAction.class),
            crateListener);

        // this listener will use the CrateRestMainAction instance and call registerHandler
        // after RestMainAction is created.
        bindListener(
            new SubclassOfMatcher(RestMainAction.class),
            new RestMainActionListener(crateListener.instanceFuture));


        bind(ClusterIdService.class).asEagerSingleton();
    }

    private class RestMainActionListener implements TypeListener {

        private final GenericBaseFuture<CrateRestMainAction> instanceFuture;

        public RestMainActionListener(GenericBaseFuture<CrateRestMainAction> instanceFuture) {
            this.instanceFuture = instanceFuture;
        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register(new InjectionListener<I>() {
                @Override
                public void afterInjection(I injectee) {
                    try {
                        CrateRestMainAction crateRestMainAction = instanceFuture.get(10, TimeUnit.SECONDS);
                        crateRestMainAction.registerHandler();
                    } catch (Exception e) {
                        logger.error("Could not register CrateRestMainAction handler", e);
                    }
                }
            });
        }
    }

    private class SubclassOfMatcher extends AbstractMatcher<TypeLiteral<?>> {

        private final Class<?> klass;

        SubclassOfMatcher(Class<?> klass) {
            this.klass = klass;
        }

        @Override
        public boolean matches(TypeLiteral<?> typeLiteral) {
            return klass.isAssignableFrom(typeLiteral.getRawType());
        }
    }

    private class CrateRestMainActionListener implements TypeListener {

        private final GenericBaseFuture<CrateRestMainAction> instanceFuture;

        public CrateRestMainActionListener() {
            this.instanceFuture = new GenericBaseFuture<>();

        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register(new InjectionListener<I>() {
                @Override
                public void afterInjection(I injectee) {
                    instanceFuture.set((CrateRestMainAction)injectee);
                }
            });
        }
    }
}
