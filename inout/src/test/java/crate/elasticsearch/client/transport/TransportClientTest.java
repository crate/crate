package crate.elasticsearch.client.transport;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Test to make sure TransportClient works correctly with InOut plugin
 */
public class TransportClientTest {


    /**
     * Instantiate a TransportClient to make sure dependency injection works correctly
     */
    @Test
    public void testTransportClient() {

        /**
         * InOut plugin modules must not be loaded for TransportClient instances
         */
        TransportClient client = new TransportClient();
        assertNotNull(client);

        /**
         * Internally, this get determined by the settings flag node.client which is set to true in case of
         * a TransportClient object. Thought the setting was given to the TransportClient with node.client = false
         * the constructor of TransportClient overwrites it to node.client = true
         */
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.client", false)
                .build();

        client = null;
        client = new TransportClient(settings);
        assertNotNull(client);

    }
}
