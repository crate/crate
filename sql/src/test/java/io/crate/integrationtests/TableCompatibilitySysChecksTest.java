/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.expression.reference.sys.check.cluster.TablesNeedRecreationSysCheck;
import io.crate.expression.reference.sys.check.cluster.TablesNeedUpgradeSysCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static io.crate.expression.reference.sys.check.AbstractSysCheck.LINK_PATTERN;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class TableCompatibilitySysChecksTest extends SQLTransportIntegrationTest {

    private void startUpNodeWithDataDir(String dataPath) throws IOException {
        Path zippedIndexDir = getDataPath(dataPath);
        Settings nodeSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(nodeSettings);
        ensureYellow();
    }

    @After
    public void shutdown() throws IOException {
        internalCluster().stopCurrentMasterNode();
    }

    @Test
    public void testRecreationRequired() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata_recreation_required.zip");
        //set license.ident setting to avoid unlicensed cluster check
        execute("set global transient 'license.ident' to 'my-key'");
        execute("select * from sys.shards");
        execute("select * from sys.checks where passed = false");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][1], is(TablesNeedRecreationSysCheck.ID));
        assertThat(response.rows()[0][3], is(SysCheck.Severity.MEDIUM.value()));
        assertThat(response.rows()[0][0],
            is(TablesNeedRecreationSysCheck.DESCRIPTION +
               "[doc.test_recreation_required, doc.test_recreation_required_parted] " +
               LINK_PATTERN + TablesNeedRecreationSysCheck.ID));
    }

    @Test
    @Ignore // TODO: Re-enable after upgrade to ES 6
    public void testUpgradeRequired() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata_upgrade_required.zip");
        //set license.ident setting to avoid unlicensed cluster check
        execute("set global transient 'license.ident' to 'my-key'");
        execute("select * from sys.checks where passed = false");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][1], is(TablesNeedUpgradeSysCheck.ID));
        assertThat(response.rows()[0][3], is(SysCheck.Severity.MEDIUM.value()));
        assertThat(response.rows()[0][0],
            is(TablesNeedUpgradeSysCheck.DESCRIPTION +
               "[doc.test_upgrade_required, doc.test_upgrade_required_parted] " +
               LINK_PATTERN + TablesNeedUpgradeSysCheck.ID));

        execute("optimize table blob.test_blob_upgrade_required, doc.test_upgrade_required, " +
                "doc.test_upgrade_required_parted with (upgrade_segments=true);");
        execute("select * from sys.checks where passed = false");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    @Ignore // TODO: Re-enable after upgrade to ES 6
    public void testAlreadyUpgraded() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata_already_upgraded.zip");
        //set license.ident setting to avoid unlicensed cluster check
        execute("set global transient 'license.ident' to 'my-key'");
        execute("select * from sys.checks where passed = false");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testBlobMetadataIsNotInvalid() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata_upgrade_required.zip");
        //set license.ident setting to avoid unlicensed cluster check
        execute("set global transient 'license.ident' to 'my-key'");
        execute("select * from information_schema.tables");
        assertTrue(response.rowCount() > 0);
    }
}
