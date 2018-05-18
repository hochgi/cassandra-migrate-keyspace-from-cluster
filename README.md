# Migrating from cassandra cluster to another

[Apache Cassandra](https://cassandra.apache.org/) is a free and open-source distributed NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure.

There is some documentation out their about how to migrate from one cluster to anoter :


* [Restoring a snapshot into a new cluster](http://docs.datastax.com/en/cassandra/2.1/cassandra/operations/ops_snapshot_restore_new_cluster.html)


Here is some script to help you do that quite easily, method doesn't care if you're restoring on the cluster or to another cluster, with same or different topology :


Create an export
-------------

* Export of keyspace schema structure with  [`DESC keyspace`](http://docs.datastax.com/en/cql/3.1/cql/cql_reference/describe_r.html)
* [Create a snapshot](http://docs.datastax.com/en/cassandra/2.1/cassandra/operations/ops_backup_takes_snapshot_t.html)
* Create a tar file with all the data
* [Remove the snapshot](http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsClearSnapShot.html)

The export script `export.sh` is doing all that, just run it like that on one of the Cassandra cluster node :

```bash
$ ./export.sh [ --data-dir <arg> ][ --host <arg> ] --keyspace <arg>

```
#### ~~EyeEm~~ hochgi's specific config settings
You can configure the cassandra data directory with `-d`/`--data-dir` options. By default, it's set to `/var/lib/cassandra/data`.
Also make sure you have the `-h`/`--host` set on each machine, referring to the internal IP. It is the easiest to `source ~/.cqlsh` from the root shell to set it (defaults to `localhost`).
And of course, you must supply the keyspace with required option `-k`/`--keyspace`.  
You can have a list of your keyspace with `desccribe keyspaces;`

All options can be listed by supplying `--help`

Transfer the tar file to one of the node of the new cluster.

Import data
-------------

Now you need to import data to do so, you have to :

* Drop the old keyspace
* Create the keyspace schema
* Import date into table with [sstableloader](https://www.datastax.com/dev/blog/bulk-loading)

That what the `import.sh` script is doing from the previous generated tar file.

```bash
$ ./import.sh --backup-dir <arg> --keypsace <arg> [ --host <arg> ]
# or:
$ ./import.sh --file <arg> [ --keypsace <arg> ][ --host <arg> ]
```

And of course all options can be listed by supplying `--help`

#### original authors:
I based this fork on top of [@tamizhgeek](https://github.com/tamizhgeek)'s & [@friedemann](https://github.com/friedemann)'s [fork](https://github.com/eyeem/cassandra-migrate-keyspace-from-cluster),   
which in turn is based on [@marty-macfly](https://github.com/marty-macfly)'s [original repo](https://github.com/linkbynet/cassandra-migrate-keyspace-from-cluster).
