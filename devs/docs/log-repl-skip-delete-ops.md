#### Context

1. Possibility to exclude DELETE operations from a publication/logical
  replication: https://github.com/crate/crate/issues/13025
2. Support for an architecture where data from multiple clusters (edge) is synchronized into a centralized cluster.

#### Assumptions

1. The primary use case is time-series and IoT data.

#### Different types of deletes that need to be handled

1. Drop table or `DROP SCHEMA ... CASCADE`

   Currently: not deleted on the subscriber (becomes a regular table).
   
   Change: not needed.
2. Delete all rows from a table with `DELETE FROM tbl`
   
   If the table isn't partitioned: results in all rows getting deleted, and one delete op per row. Handled with (4).

   If the table is partitioned: results in all partitions being deleted (goes back to (3) below).
3. Delete a partition with `DELETE FROM tbl WHERE <condition>` and `condition` is a query that matches a whole
   partition.

   Currently: results in the partition being deleted in one shot.

   Change: tracking stops, but the partition should be kept on the sub.
4. Delete rows using a query that isn't (3) above.
   
    Currently: each deleted row results in a delete op.

    Change: each deleted row needs to be skipped.
5. Drop a column with `ALTER TABLE DROP COLUMN`

   This is an open question. When a column is dropped, the data remains on disk,
   see https://github.com/crate/crate/issues/14254. 

#### How to handle the deleted rows

Related to point (4) above.

##### How to handle inserting the same ID again

When a table has a primary key, then the following steps result in the same `_id` re-inserted:

- insert id = 1
- delete id = 1
- insert id = 1

**Options**

1. **Not handle the case at all**

   This is due to assumption 1 (the primary use case is time-series and IoT data). There is still one possible use case,
   and that is fixing data through deleting and re-inserting. It's not clear if this needs to be supported though.
   Fixing data through updates would still be supported.

   If it *does* happen, the result is a duplicate `_id`/primary key.

2. **Fail**

   Pg stops replication on conflicts: https://www.postgresql.org/docs/current/logical-replication-conflicts.html. Same
   in MySQL,
   see [this](https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html#sysvar_replica_exec_mode)
   and [this](https://dev.mysql.com/doc/refman/8.4/en/replication-features-errors.html).

   This means that we need to have an efficient way to check if the subscriber skipped a delete op for a given `_id`.

   A user needs to be notified about the failure, maybe through `pg_subscription_rel.srsubstate`. We also need to
   provide a way to a user to fix the failure, e.g. skip the operation. Updating/deleting data manually on a
   subscription table isn't supported.

3. **Skip new inserts for the same _id** (i.e., if a delete op for the same `_id` was already skipped)

    Example:   

    - insert id 1 (value A)
    - delete id 1
    - insert id 1 (value B)

   This would keep value A (original) on the subscriber, and value B on the publisher. If the `delete + insert` cycle
   would repeat, then the subscriber would always keep the first value, and the publisher would contain the latest
   value. The values in between would be lost.

    Another example:
    - insert id 1 (value A)
    - delete id 1
    - insert id 1 (value B)
    - update id 1 (value C)
      
    If `value B` is skipped, then for `value C` we can:
    - Apply the update. The value on the sub and on the pub would be the same. If the update didn't happen, the pub and
      sub would have different values. So, depending on the type of changes, subscriber and publisher may or may not
      display the same latest values.
    - Skip the update as well. Then we're stuck with value A forever, which is wrong.
      
4. **Overwrite the doc with the same _id**, (i.e., if a delete op for the same `_id` was already skipped)

   This has the same effect as applying the delete op, which is what we want to avoid.

**Discussion**

Option 4 would mean the delete ops were applied, even though the pub was configured to leave them out, so that option is
dismissed. Option 3 is rather confusing: in some cases the sub has the same values as the pub, in some it doesn't.

Between 1 and 2, I lean towards 1, since the assumption is that the primary use case are time-series and IoT. Option 1
is also cheaper (it needs to lookups/checks as the option 2 does). Option 2 can be the next step.

##### Skipping seq numbers in the subscriber is not possible

What is meant by "skipping" is: when the subscriber receives a delete op, it just moves to the next op.

The subscriber's local checkpoint cannot advance if there's a gap in seq. numbers
(see [this](https://github.com/crate/crate/blob/3e7490b0fef082ed3cb8213c7a441129c60fb1ff/server/src/main/java/org/elasticsearch/index/seqno/LocalCheckpointTracker.java#L134-L136)).
This means that every seq_no must be applied as *some* operation on the subscriber. That operation can be the skipped
operation itself, or a `NoOp`, etc.

#### Where should the delete operations be skipped

**Options**:

1. **On the subscriber**
   
    The publication sends all the events, but the subscriber filters. 
    
    Pros:
    - Only the subscriber needs an upgrade, publishers can stay as they are.
    
    Cons: 
    - Performance is worse compared to the other option, because there's more traffic. Especially true when/if we decide
      to skip inserts or updates. Mass deletes might be an issue as well.

2. **On the publisher**

   The publisher sends only the published type of changes. For example, if `delete` ops are excluded, then the publisher
   sends only the inserts and updates.
    
    Pros:
    - Better performance, allows for "compression of events". E.g. the publisher can send an event like
      `SkipOps from 1 to 200`.
    
    Cons:
    - Both pub and sub need to be upgraded.

**Discussion**

The only advantage of filtering the ops on the subscriber is upgrading. However, that's a relatively small advantage:
the upgrade will need to be done at some point, and we can handle it well with the usual mechanism (checking the in/out
versions). For performance reasons, it's better that a publication sends only the ops that are configured, so the
preferred option here is **on the publisher**.

#### Configuring which operations should be published

**Notes**:

1. We can't differentiate between inserts and updates. So, whenever inserts are published, updates need to be published
   as well.
2. If we allow inserts and updates to be skipped, only deletes are left, which doesn't make much sense. So it's either:
   delete are published or deletes are not published.

**Options**

1. Specify events to be published, e.g. `(publish = 'insert, update')`
2. Specify events to be skipped. e.g. `(exclude = 'delete')`

   Pg doesn't have this, only a `publish` list.
3. Mention deletes specifically, e.g. `(publish_deletes = true)`.

**Discussion**

Option 3 is clear about what's possible. However, if in the future, we ever make it possible to publish inserts or
updates, then this option will be deprecated.

Neither option 1 nor option 2 are better when it comes to the notes mentioned above. Option 1 is similar to what Pg
does, so it's the preferred option here.

#### Configuring which types of delete operations need to be skipped

1. One option for all types of deletes (deleted rows, deleted partitions).
2. One option for each type (`delete_row`, `delete_partition`).

**Discussion**

The options above aren't mutually exclude. We can have `publish = delete` and `publish = delete_partition`. The code
paths for (not) publishing deleted rows and handling deleted partitions are clearly split, so we can start
differentiating between those at any time. 

Given our assumption (time-series and IoT data), it's not clear if there's a use case for deleting data except for a
cleanup (in which case we want the data to be kept on the central cluster/subscriber).

Hence, the preferred option here is to have a single option, `delete`, that handles all types of deletes. 

#### Open questions

1. What happens with recreated tables?
2. What happens with recreated partitions?
3. How to delete data on the subscriber/central cluster from the issue's scenario?
4. Should `ALTER PUBLICATION` support changing the published actions? If some data on the publisher cluster is "bad",
   and needs to be fixed (re-inserted), then allowing all operations would make it possible to fix the data in the
   subscriber too.
5. If the subscription stops/fails on an `_id` that has been re-inserted, can
   `index.soft_deletes.retention_lease.period` get into the way if an error hasn't been resolved for a long time (the
   subscription is paused)?
