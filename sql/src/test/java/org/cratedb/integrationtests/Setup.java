package org.cratedb.integrationtests;

import org.cratedb.test.integration.AbstractSharedCrateClusterTest;

import static org.cratedb.test.integration.PathAccessor.stringFromPath;

public class Setup {

    private final AbstractSharedCrateClusterTest test;

    public Setup(AbstractSharedCrateClusterTest test) {
        this.test = test;
    }

    public void setUpLocations() throws Exception {
        test.prepareCreate("locations").setSettings(
                test.randomSettingsBuilder().loadFromClasspath("/essetup/settings/test_a.json").build())
                .addMapping("default", stringFromPath("/essetup/mappings/test_a.json", Setup.class))
                .execute().actionGet();
        test.loadBulk("/essetup/data/test_a.json", Setup.class);
        test.refresh();
    }
}
