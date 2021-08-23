
package io.crate.types;

import java.util.List;

import io.crate.metadata.Reference;

// TODO: does it make sense to abstract away the engine and query type?
public interface StorageReader<T, U> {

    T termQuery(Reference ref, U value);

    T termsQuery(Reference ref, List<U> values);

    T rangeQuery(Reference ref, U lower, U upper, boolean includeLower, boolean includeUpper);

    T existsQuery(Reference ref);
}
