/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operator.scalar;

import io.crate.operator.Input;
import io.crate.operator.reference.sys.shard.ShardTableNameExpression;
import io.crate.planner.symbol.LongLiteral;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteFunctionTest {

    private Injector injector;

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(Settings.class).toInstance(ImmutableSettings.EMPTY);

            TransportDeleteAction transportDeleteAction = mock(TransportDeleteAction.class);
            AdapterActionFuture adapterActionFuture = mock(AdapterActionFuture.class);
            DeleteResponse deleteResponse = mock(DeleteResponse.class);
            when(deleteResponse.isNotFound()).thenReturn(false);
            when(adapterActionFuture.actionGet()).thenReturn(deleteResponse);
            when(transportDeleteAction.execute(any(DeleteRequest.class))).thenReturn(adapterActionFuture);
            bind(TransportDeleteAction.class).toInstance(transportDeleteAction);

        }
    }

    class IdSysColumnReference implements Input<BytesRef> {
        @Override
        public BytesRef value() {
            return new BytesRef("1");
        }
    }


    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new TestModule()
        ).createInjector();
    }

    @Test
    public void testDelete() throws Exception {
        ShardTableNameExpression shardTableNameExpression = new ShardTableNameExpression(new ShardId("test", 0));

        DeleteFunction deleteFunction = new DeleteFunction(injector.getInstance(TransportDeleteAction.class));

        assertTrue(deleteFunction.evaluate(shardTableNameExpression, new IdSysColumnReference()));
    }

    @Test
    public void testDeleteWithVersion() throws Exception {
        LongLiteral requiredVersion = new LongLiteral(1L);
        ShardTableNameExpression shardTableNameExpression = new ShardTableNameExpression(new ShardId("test", 0));

        DeleteFunction deleteFunction = new DeleteFunction(injector.getInstance(TransportDeleteAction.class));

        assertTrue(deleteFunction.evaluate(shardTableNameExpression, new IdSysColumnReference(), requiredVersion));
    }
}
