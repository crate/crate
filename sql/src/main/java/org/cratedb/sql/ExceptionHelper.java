package org.cratedb.sql;

import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
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
}
