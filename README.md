# Migrating from cassandra cluster to another

[Apache Cassandra](https://cassandra.apache.org/) is a free and open-source distributed NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure.

There is some documentation out their about how to migrate from one cluster to anoter :


* [Restoring a snapshot into a new cluster](http://docs.datastax.com/en/cassandra/2.1/cassandra/operations/ops_snapshot_restore_new_cluster.html)


Here is some script to help you do that quite easily, method doesn't care if you're restoring on the cluster or to another cluster, with same or different topology :


Create an export
-------------

* Export of keyspace & specific table schema structure with  [`DESC keyspace.table`](http://docs.datastax.com/en/cql/3.1/cql/cql_reference/describe_r.html)
* [Create a snapshot](http://docs.datastax.com/en/cassandra/2.1/cassandra/operations/ops_backup_takes_snapshot_t.html)
* Create tar file or directory with all the data
* [Remove the snapshot](http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsClearSnapShot.html)

The export script `export-init` is doing all that, just run it like that on one of the Cassandra cluster node :

```bash
$ ./export-init [ OPTIONS ] --keyspace <arg> --table <arg>
```
#### Specific config settings
- You can configure the cassandra data directory with `-d`/`--data-dir` options. By default, it's set to `/var/lib/cassandra/data`.
- Also make sure you have the `-h`/`--host` set on each machine, referring to the internal IP. It is the easiest to `source ~/.cqlsh` from the root shell to set it (defaults to `localhost`).
- If you want a compressed file for backup, use `-z`/`--zip-files`, otherwise, directory is left uncompressed (ready to be `rsync`ed  for example)
- And of course, you must supply the keyspace with required option `-k`/`--keyspace`. You can have a list of your keyspace with `desccribe keyspaces;`
- And obviously the required table option `-t`/`--table`
- Other options are specifying cassandra's JMX port with `-j`/`--jmx-port` (defaults to 7199), and cqlsh port with `-p`/`--port` (defaults to 9042)
- All options can be listed by supplying `--help`

Import data
-------------

Now you need to import data to do so, you have to :

* Create the keyspace (if not exist) & table schema
* Import date into table with [sstableloader](https://www.datastax.com/dev/blog/bulk-loading)

That what the `import-init` script is doing from the previous generated tar file / backup dir.

```bash
$ ./import-init --backup-dir <arg> --keypsace <arg> [ --host <arg> ]
# or:
$ ./import-init --file <arg> [ --keypsace <arg> ][ --host <arg> ]
```

And of course all options can be listed by supplying `--help`

Incremantal table updates
-------------------------

After you've initially loaded tables data into your backup cassandra cluster,
you'll probably want to update it incrementally. To do that, theres also a cassandra stream application you can use.
It is built using [sbt](https://www.scala-sbt.org), and you can simply run it using:
```sh
$ sbt run
```
You'll need to configure your queries.
This tool assumes you know how to define CQL queries such that you only fetching new data.  
Configuration file is located in `src/main/resources/application.conf`.
You can configure the remote cluster to fetch data from, the local cluster to push the data to, and the CQL queries to SELECT & INSERT data, along with all the keys to bind.

### common use-case (N:N - copy from same size cluster):
##### First time only:
- `cssh` onto remote cluster.
- use `./export-init`
- `cssh` onto local cluster
- `rsync -Pvaz user@remote:path/to/bkp-1234 /some/local/path`
- use `import-init`

##### Every consecutive update (any size cluster; can be set as a chron job on your CI server for example)
- set keys (either update `application.conf` or use system properties)
- `sbt run`

#### original authors:
I based this fork on top of [@tamizhgeek](https://github.com/tamizhgeek)'s & [@friedemann](https://github.com/friedemann)'s [fork](https://github.com/eyeem/cassandra-migrate-keyspace-from-cluster),   
which in turn is based on [@marty-macfly](https://github.com/marty-macfly)'s [original repo](https://github.com/linkbynet/cassandra-migrate-keyspace-from-cluster).
