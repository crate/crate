package io.crate.metadata;

import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;

public interface FunctionFactory {

    FunctionImplementation create(Signature signature, BoundSignature boundSignature);
}
