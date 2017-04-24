package io.crate.protocols.postgres;


import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.channel.Channel;

/**
 * Common interface for Authentication methods.
 *
 * An auth method must provide a unique name which is exposed via the {@link #name()} method.
 *
 * It is also responsible for authentication for the Postgres Wire Protocol,
 * {@link #pgAuthenticate(Channel channel, Settings settings)},
 */
public interface AuthenticationMethod {
    /**
     * Authenticates the Postgres Wire Protocol client,
     * sends AuthenticationOK if authentication is successful
     * If authentication fails it send ErrorResponse
     * @param channel request channel
     * @param settings from the cluster state
     */
    void pgAuthenticate(Channel channel, Settings settings);


    /**
     * @return name of the authentication method
     */
    String name();
}
