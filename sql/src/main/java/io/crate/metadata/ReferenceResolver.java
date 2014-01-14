package io.crate.metadata;

public interface ReferenceResolver {

    public ReferenceInfo getInfo(ReferenceIdent ident);

    public ReferenceImplementation getImplementation(ReferenceIdent ident);

}
