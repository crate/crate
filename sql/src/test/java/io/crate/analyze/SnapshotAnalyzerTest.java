/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.metadata.MetaDataModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

public class SnapshotAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                        new MockedClusterServiceModule(),
                        new MetaDataModule(),
                        new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testSimpleCreateSnapshot() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("cannot analyze statement: 'CreateSnapshot{name=my_repo.my_snapshot, properties=Optional.absent(), tableList=Optional.absent()}'");
        analyze("CREATE SNAPSHOT my_repo.my_snapshot ALL");
    }

    @Test
    public void testSimpleDropSnapshot() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("cannot analyze statement: 'DropSnapshot{name=my_repo.my_snapshot}'");
        analyze("DROP SNAPSHOT my_repo.my_snapshot");
    }

    @Test
    public void testSimpleRestoreSnapshot() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("cannot analyze statement: 'RestoreSnapshot{name=my_repo.my_snapshot, properties=Optional.absent(), tableList=Optional.absent()}'");
        analyze("RESTORE SNAPSHOT my_repo.my_snapshot ALL");
    }
}
