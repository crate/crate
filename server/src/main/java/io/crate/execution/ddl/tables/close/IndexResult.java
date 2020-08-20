package io.crate.execution.ddl.tables.close;


import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nullable;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;

public final class IndexResult implements Writeable {

      private final Index index;
      private final @Nullable Exception exception;
      private final @Nullable ShardResult[] shards;

      public IndexResult(final Index index) {
          this(index, null, null);
      }

      public IndexResult(final Index index, final Exception failure) {
          this(index, Objects.requireNonNull(failure), null);
      }

      public IndexResult(final Index index, final ShardResult[] shards) {
          this(index, null, Objects.requireNonNull(shards));
      }

      private IndexResult(final Index index, @Nullable final Exception exception, @Nullable final ShardResult[] shards) {
          this.index = Objects.requireNonNull(index);
          this.exception = exception;
          this.shards = shards;
      }

      IndexResult(final StreamInput in) throws IOException {
          this.index = new Index(in);
          this.exception = in.readException();
          this.shards = in.readOptionalArray(ShardResult::new, ShardResult[]::new);
      }

      @Override
      public void writeTo(final StreamOutput out) throws IOException {
          index.writeTo(out);
          out.writeException(exception);
          out.writeOptionalArray(shards);
      }

      public Index getIndex() {
          return index;
      }

      public Exception getException() {
          return exception;
      }

      public ShardResult[] getShards() {
          return shards;
      }

      public boolean hasFailures() {
          if (exception != null) {
              return true;
          }
          if (shards != null) {
              for (ShardResult shard : shards) {
                  if (shard.hasFailures()) {
                      return true;
                  }
              }
          }
          return false;
      }

}
