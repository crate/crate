
package io.crate.protocols.postgres;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import io.crate.netty.EventLoopGroups;

/**
 * Used to create {@link PgClient} instances.
 *
 * The main intention of this class is to hide dependencies of a PgClient from components
 * which may need to create a PgClient.
 **/
@Singleton
public class PgClientFactory {

    private final Settings settings;
    private final EventLoopGroups eventLoopGroups;

    @Inject
    public PgClientFactory(Settings settings,
                           EventLoopGroups eventLoopGroups) {
        this.settings = settings;
        this.eventLoopGroups = eventLoopGroups;
    }

    public PgClient createClient(String host) {
        return new PgClient(settings, eventLoopGroups, host);
    }
}
