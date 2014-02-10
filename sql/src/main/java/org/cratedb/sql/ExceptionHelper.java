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

package org.cratedb.sql;

import org.cratedb.Constants;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.transport.RemoteTransportException;

public class ExceptionHelper {

    /**
     * Returns the cause throwable of a {@link org.elasticsearch.transport.RemoteTransportException}
     * and {@link org.elasticsearch.action.search.ReduceSearchPhaseException}.
     * Also transform throwable to {@link org.cratedb.sql.CrateException}.
     *
     * @param e
     * @return
     */
    public static Throwable transformToCrateException(Throwable e) {
        if (e instanceof RemoteTransportException) {
            // if its a transport exception get the real cause throwable
            e = e.getCause();
        }

        if (e instanceof DocumentAlreadyExistsException) {
            return new DuplicateKeyException(
                    "A document with the same primary key exists already", e);
        } else if (e instanceof IndexAlreadyExistsException) {
            return new TableAlreadyExistsException(((IndexAlreadyExistsException)e).index().name(), e);
        } else if ((e instanceof InvalidIndexNameException)) {
            if (e.getMessage().contains("an alias with the same name already exists")) {
                // treat an alias like a table as aliases are not officially supported
                return new TableAlreadyExistsException(((InvalidIndexNameException)e).index().getName(),
                        e);
            }
            return new InvalidTableNameException(((InvalidIndexNameException) e).index().getName(), e);
        } else if (e instanceof IndexMissingException) {
            return new TableUnknownException(((IndexMissingException)e).index().name(), e);
        } else if (e instanceof ReduceSearchPhaseException && e.getCause() instanceof VersionConflictException) {
            /**
             * For update or search requests we use upstream ES SearchRequests
             * These requests are executed using the transportSearchAction.
             *
             * The transportSearchAction (or the more specific QueryThenFetch/../ Action inside it
             * executes the TransportSQLAction.SearchResponseListener onResponse/onFailure
             * but adds its own error handling around it.
             * By doing so it wraps every exception raised inside our onResponse in its own ReduceSearchPhaseException
             * Here we unwrap it to get the original exception.
             */
            return e.getCause();
        }
        return e;
    }

    /**
     * Throws an {@link org.cratedb.sql.CrateException} if a
     * {@link org.elasticsearch.action.search.ShardSearchFailure} occurs.
     *
     * @param shardSearchFailures
     * @throws CrateException
     */
    public static void exceptionOnSearchShardFailures(ShardSearchFailure[] shardSearchFailures) throws CrateException {
        for (ShardSearchFailure failure : shardSearchFailures) {
            if (failure.failure().getCause() instanceof VersionConflictEngineException) {
                throw new VersionConflictException(failure.failure());
            }
        }

        // just take the first failure to have at least some stack trace.
        throw new CrateException(shardSearchFailures.length + " shard failures",
                shardSearchFailures[0].failure());

    }

    /**
     * Returns a {@link DeleteResponse} with attribute notFound set to true if a
     * {@link VersionConflictEngineException} occurs.
     * Intention is that we want to response a valid {@link org.cratedb.action.sql.SQLResponse}
     * with rowCount 0 instead of responding an error.
     *
     * @param e
     * @return
     */
    public static DeleteResponse deleteResponseFromVersionConflictException(Throwable e) {
        DeleteResponse deleteResponse = null;
        if (e instanceof RemoteTransportException) {
            // if its a transport exception get the real cause throwable
            e = e.getCause();
        }
        if (e instanceof VersionConflictEngineException) {
            VersionConflictEngineException ex = (VersionConflictEngineException) e;
            deleteResponse = new DeleteResponse(
                    ex.index().getName(),
                    Constants.DEFAULT_MAPPING_TYPE,
                    "1", // dummy id since we cannot know it here, not used anywhere so its' ok
                    ex.getCurrentVersion(),
                    true);
        }
        return deleteResponse;
    }
}
