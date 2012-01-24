cascading-dbmigrate
===================

cascading-dbmigrate makes it easy to run Cascading flows on sql tables with a
primary key of an int or a long. We use it at BackType to migrate data from our
databases to HDFS.

Cascading-DBMigrate is available [on Clojars](http://clojars.org/backtype/cascading-dbmigrate).

Usage
-----

To read data from a database in a Cascading flow, use DBMigrateTap.
DBMigrateTap's constructor has the following signature:

    DBMigrateTap(
      int numChunks,        // The number of splits to create of the database.
                            // This will correspond to the number of mappers
                            // created to read the database.
      String dbDriver,      // For example, "com.mysql.jdbc.Driver"
      String dbUrl,         // For example, "jdbc:mysql://localhost:3306/mydb"
      String username,      // Username to connect to your database.
      String pwd,           // Password to connect to your database.
      String tableName,     // The table to read during the flow.
      String pkColumn,      // The name of the primary key column of the table.
      String[] columnNames, // The names of the columns to read into the flow.
      Options ops           // Optional, can provide min/max values to read.
    )

The tap will emit tuples containing one field for each column read, the field
names being the column names.

Examples
--------

### Cascalog

```clojure
 (defn db-range [min max]
   (let [opts (new cascading.dbmigrate.tap.DBMigrateTap$Options)]
     (set! (. opts :minId) min))
     (set! (. opts :maxId) max))
     opts))

 (defn db-tap [table]
   (cascading.dbmigrate.tap.DBMigrateTap.
     1
     "com.mysql.jdbc.Driver"
     "jdbc:mysql://localhost:3306/mydb"
     "root"
     ""
     table
     "id"
     (into-array ["id" "name"])
     (db-range 1 100))) ;; Only load first 100 records


 (?<- (stdout)
      [?id ?name]
      ((db-tap "users") ?id ?name))
```

Building
--------

To build cascading-dbmigrate, follow these instructions:

1. Set `HADOOP_HOME` environment variable to the root directory of your hadoop distribution.
2. Set `CASCADING_HOME` environment variable to the root directory of your cascading distribution.
3. `ant jar`

This will produce a single jar called `cascading_dbmigrate.jar` in the `build/`
directory.


Thanks to Chris Wensel for his help in developing this project.

