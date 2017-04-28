package io.crate.operation.reference.sys.cluster;


import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthenticationSettingsTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        return new SQLPlugin(Settings.EMPTY).getSettings();
    }

    @Test
    public void testHBASettingIsApplied() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.a.method", "trust")
            .put("auth.host_based.a.user", "crate")
            .put("auth.host_based.a.address", "172.0.0.1")
            .build();
        // build cluster service mock to pass initial settings
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);

        Settings clusterSettings = clusterService.getSettings();

        assertThat(clusterSettings.get("auth.host_based.a.user"),
            is("crate"));
        assertThat(clusterSettings.get("auth.host_based.a.method"),
            is("trust"));
        assertThat(clusterSettings.get("auth.host_based.a.address"),
            is("172.0.0.1"));

    }

    @Test
    public void testHBAEmptyFields() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.a.method", "md5")
            .build();
        // build cluster service mock to pass initial settings
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(settings);

        Settings clusterSettings = clusterService.getSettings();

        assertEquals(clusterSettings.get("auth.host_based.a.user"),
           null);
        assertThat(clusterSettings.get("auth.host_based.a.method"),
            is("md5"));
        assertEquals(clusterSettings.get("auth.host_based.a.address"),
            null);

    }
}
