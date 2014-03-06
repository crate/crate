/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.module.sql.test;

import io.crate.exceptions.*;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExceptionHelperTest {

    class TestException extends Throwable {}

    @Test (expected = TestException.class)
    public void testReRaiseCrateExceptionGeneric() throws Throwable {
        Throwable throwable = new TestException();
        throw ExceptionHelper.transformToCrateException(throwable);
    }

    @Test (expected = TestException.class)
    public void testReRaiseCrateExceptionRemoteTransportGeneric() throws Throwable {
        Throwable throwable = new TestException();
        throw ExceptionHelper.transformToCrateException(
                new RemoteTransportException("remote transport failed", throwable)
        );
    }
    @Test (expected = DuplicateKeyException.class)
    public void testReRaiseCrateExceptionDocumentAlreadyExists() throws Throwable {
        Throwable throwable = new DocumentAlreadyExistsException(
                new ShardId("test", 1),
                "default",
                "1"
               );
        throw ExceptionHelper.transformToCrateException(throwable);
    }

    @Test (expected = TableAlreadyExistsException.class)
    public void testReRaiseCrateExceptionIndexAlreadyExists() throws Throwable {
        Throwable throwable = new IndexAlreadyExistsException(new Index("test"));
        throw ExceptionHelper.transformToCrateException(throwable);
    }

    @Test (expected = VersionConflictException.class)
    public void testReRaiseCrateExceptionReduceSearchPhaseVersionConflict() throws Throwable {
        Throwable throwable1 = new VersionConflictException(new Throwable());

        ShardSearchFailure[] searchFailures = {new ShardSearchFailure(throwable1)};

        throw ExceptionHelper.transformToCrateException(
                new ReduceSearchPhaseException("step1", "reduce failed", throwable1,
                        searchFailures)
        );
    }

    @Test (expected = CrateException.class)
    public void testOnShardSearchFailures() throws Exception {
        ShardSearchFailure[] shardSearchFailures = {
                new ShardSearchFailure(new Exception())
        };
        ExceptionHelper.exceptionOnSearchShardFailures(shardSearchFailures);
    }

    @Test (expected = VersionConflictException.class)
    public void testOnShardSearchFailuresVersionConflict() throws Exception {
        VersionConflictEngineException versionConflictEngineException =
                new VersionConflictEngineException(new ShardId("test",1), "default", "1", 1, 1);
        Exception failureException = new RemoteTransportException(
                "failed",
                versionConflictEngineException);
        ShardSearchFailure[] shardSearchFailures = {
                new ShardSearchFailure(failureException)
        };
        ExceptionHelper.exceptionOnSearchShardFailures(shardSearchFailures);
    }

    @Test (expected = TableUnknownException.class)
    public void testReRaiseCrateExceptionIndexMissing() throws Throwable {
        Throwable throwable = new IndexMissingException(new Index("test"));
        throw ExceptionHelper.transformToCrateException(throwable);
    }

    @Test
    public void deleteResponseFromVersionConflictException() throws Throwable {
        VersionConflictEngineException e =
                new VersionConflictEngineException(new ShardId("test",1), "default", "1", 1, 1);
        DeleteResponse deleteResponse =  ExceptionHelper.deleteResponseFromVersionConflictException(e);
        assertTrue(deleteResponse instanceof DeleteResponse);
        assertNotNull(deleteResponse);
    }

    @Test
    public void deleteResponseFromVersionConflictRemoteException() throws Throwable {
        VersionConflictEngineException versionConflictEngineException =
                new VersionConflictEngineException(new ShardId("test",1), "default", "1", 1, 1);
        Exception e = new RemoteTransportException("failed", versionConflictEngineException);
        DeleteResponse deleteResponse =  ExceptionHelper.deleteResponseFromVersionConflictException(e);
        assertTrue(deleteResponse instanceof DeleteResponse);
        assertNotNull(deleteResponse);
    }

    @Test
    public void deleteResponseFromVersionConflictExceptionNone() throws Throwable {
        Exception e = new Exception();
        DeleteResponse deleteResponse =  ExceptionHelper.deleteResponseFromVersionConflictException(e);
        assertNull(deleteResponse);
    }

}
