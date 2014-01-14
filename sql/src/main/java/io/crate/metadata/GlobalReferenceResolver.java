package io.crate.metadata;

import com.google.common.base.Preconditions;
import io.crate.metadata.sys.SystemReferences;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class GlobalReferenceResolver implements ReferenceResolver {

    private final Map<ReferenceIdent, ReferenceImplementation> implementations;

    @Inject
    public GlobalReferenceResolver(Map<ReferenceIdent, ReferenceImplementation> implementations) {
        this.implementations = implementations;
    }

    @Override
    public ReferenceInfo getInfo(ReferenceIdent ident) {
        // TODO: register a resolver for each schema?
        String schema = ident.tableIdent().schema();
        Preconditions.checkArgument(SystemReferences.SCHEMA.equals(schema),
                "Table schema not supported", schema);
        return SystemReferences.get(ident);
    }

    @Override
    public ReferenceImplementation getImplementation(ReferenceIdent ident) {
        if (ident.isColumn()) {
            return implementations.get(ident);
        }
        ReferenceImplementation impl = implementations.get(ident.columnIdent());
        if (impl != null) {
            for (String part : ident.path()) {
                impl = impl.getChildImplementation(part);
            }
        }
        return impl;
    }

}
