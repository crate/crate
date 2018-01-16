package io.crate.execution.expression.reference.sys.check.cluster;

import io.crate.execution.expression.reference.sys.check.SysCheck;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class LicenseEnterpriseChecksTest extends CrateUnitTest {
    private ClusterSettings clusterSettings;

    @Before
    public void registerSettings() {
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.addAll(sqlPlugin.getSettings());
        clusterSettings = new ClusterSettings(Settings.EMPTY, settings);
    }

    @Test
    public void testCommunityEdition() throws Exception {
        Settings settings = Settings.builder().put("license.enterprise", false).build();
        LicenseEnterpriseChecks licenseEnterpriseCheck = new LicenseEnterpriseChecks(clusterSettings, settings);

        assertThat(licenseEnterpriseCheck.id(), Is.is(4));
        assertThat(licenseEnterpriseCheck.severity(), Is.is(SysCheck.Severity.LOW));
        assertThat(licenseEnterpriseCheck.validate(), Is.is(true));
    }

    @Test
    public void testUnlicensencedEnterpriseEdition() throws Exception {
        Settings settings = Settings.builder().put("license.enterprise", true).build();
        LicenseEnterpriseChecks licenseEnterpriseCheck = new LicenseEnterpriseChecks(clusterSettings, settings);
        assertThat(licenseEnterpriseCheck.validate(), Is.is(false));
    }

    @Test
    public void testLicensencedEnterpriseEdition() throws Exception {
        Settings settings = Settings.builder()
            .put("license.enterprise", true)
            .put("license.ident", "my-awesome-key").build();

        LicenseEnterpriseChecks licenseEnterpriseCheck = new LicenseEnterpriseChecks(clusterSettings, settings);
        assertThat(licenseEnterpriseCheck.validate(), Is.is(true));
    }

    @Test
    public void testLicensencedEnterpriseUpdateIdent() throws Exception {
        Settings settings = Settings.builder()
            .put("license.enterprise", true).build();

        LicenseEnterpriseChecks licenseEnterpriseCheck = new LicenseEnterpriseChecks(clusterSettings, settings);
        assertThat(licenseEnterpriseCheck.validate(), Is.is(false));

        Settings newSettings = Settings.builder()
            .put("license.ident", "my new license").build();

        clusterSettings.applySettings(newSettings);
        assertThat(licenseEnterpriseCheck.validate(), Is.is(true));
    }

}
