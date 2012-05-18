/**
Copyright 2010 BackType

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/
package cascading.dbmigrate.tap;

import cascading.dbmigrate.hadoop.DBInputFormat;
import cascading.dbmigrate.hadoop.TupleWrapper;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import java.io.Serializable;
import java.util.Map;

public class DBMigrateTap extends Tap {
    public static class Options implements Serializable {
        public Long minId = null;
        public Long maxId = null;
        public Map<String,String> driverProps;
    }

    public class DBMigrateScheme extends Scheme {
        String dbDriver;
        String dbUrl;
        String username;
        String pwd;
        String tableName;
        String pkColumn;
        String[] columnNames;
        int numChunks;
        Options options;

        public DBMigrateScheme(int numChunks, String dbDriver, String dbUrl, String username, String pwd, String tableName, String pkColumn, String[] columnNames, Options options) {
            super(new Fields(columnNames));
            this.dbDriver = dbDriver;
            this.dbUrl = dbUrl;
            this.username = username;
            this.pwd = pwd;
            this.tableName = tableName;
            this.pkColumn = pkColumn;
            this.columnNames = columnNames;
            this.numChunks = numChunks;
            this.options = options;
        }

        @Override
        public void sourceInit(Tap tap, JobConf jc) throws IOException {
            // a hack for MultiInputFormat to see that there is a child format
            FileInputFormat.setInputPaths( jc, getPath() );

            DBInputFormat.setInput(jc, numChunks, dbDriver, username, pwd, dbUrl, tableName, pkColumn, options.minId, options.maxId, options.driverProps, columnNames);
        }

        @Override
        public void sinkInit(Tap tap, JobConf jc) throws IOException {
            throw new UnsupportedOperationException("Cannot be used as a sink");
        }

        @Override
        public Tuple source(Object key, Object val) {
            return ((TupleWrapper) val).tuple;
        }

        @Override
        public void sink(TupleEntry te, OutputCollector oc) throws IOException {
            throw new UnsupportedOperationException("Cannot be used as a sink.");
        }
    }

    String connectionUrl;

    public DBMigrateTap(int numChunks, String dbDriver, String dbUrl, String username, String pwd, String tableName, String pkColumn, String[] columnNames) {
        this(numChunks, dbDriver, dbUrl, username, pwd, tableName, pkColumn, columnNames, new Options());
    }

    public DBMigrateTap(int numChunks, String dbDriver, String dbUrl, String username, String pwd, String tableName, String pkColumn, String[] columnNames, Options options) {
        setScheme(new DBMigrateScheme(numChunks, dbDriver, dbUrl, username, pwd, tableName, pkColumn, columnNames, options));
        this.connectionUrl = dbUrl;
    }

    @Override
    public Path getPath() {
        return new Path( "jdbc:/" + connectionUrl.replaceAll( ":", "_" ) );
    }

    @Override
    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
        return new TupleEntryIterator(getSourceFields(), new TapIterator(this, conf));
    }

    @Override
    public TupleEntryCollector openForWrite(JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean makeDirs(JobConf jc) throws IOException {
        return true;
    }

    @Override
    public boolean deletePath(JobConf jc) throws IOException {
        return false;
    }

    @Override
    public boolean pathExists(JobConf jc) throws IOException {
        return true;
    }

    @Override
    public long getPathModified(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

}
