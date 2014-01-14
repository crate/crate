package io.crate.metadata;

public interface ReferenceImplementation {

    public abstract ReferenceInfo info();

    /**
     * Returns an implemnentation for a child.
     *
     * @param name The name of the child
     * @return an implementation for the child or null if not applicable or if there is no child available
     * with the given name
     */
    public ReferenceImplementation getChildImplementation(String name);

}
