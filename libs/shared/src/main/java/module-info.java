module io.crate.shared {
    requires org.apache.logging.log4j;
    requires org.jetbrains.annotations;

    exports io.crate.common;
    exports io.crate.common.annotations;
    exports io.crate.common.collections;
    exports io.crate.common.io;
    exports io.crate.common.unit;
    exports io.crate.exceptions;
}
