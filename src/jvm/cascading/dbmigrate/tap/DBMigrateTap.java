/**
Copyright 2010 BackType

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/
package cascading.dbmigrate.tap;

import cascading.dbmigrate.hadoop.DBInputFormat;
import cascading.dbmigrate.hadoop.TupleWrapper;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.MultiRecordReaderIterator;
import cascading.tap.hadoop.RecordReaderIterator;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.Serializable;


public class DBMigrateTap extends Hfs {
    public static class Options implements Serializable {
        public Long minId = null;
        public Long maxId = null;
    }
    
    public class DBMigrateScheme extends Scheme<HadoopFlowProcess, JobConf, RecordReader, OutputCollector, Object[], Object[]> {
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
        public void sourceConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
            // a hack for MultiInputFormat to see that there is a child format
            FileInputFormat.setInputPaths( conf, getPath() );
            DBInputFormat.setInput(conf, numChunks, dbDriver, username, pwd, dbUrl, tableName, pkColumn, options.minId, options.maxId, columnNames);
        }

        @Override
        public void sinkConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
            throw new UnsupportedOperationException("Cannot be used as a sink");
        }

        @Override public void sourcePrepare(HadoopFlowProcess flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {

            sourceCall.setContext(new Object[2]);

            sourceCall.getContext()[0] = sourceCall.getInput().createKey();
            sourceCall.getContext()[1] = sourceCall.getInput().createValue();
        }

        @Override
        public boolean source(HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
            Object key = sourceCall.getContext()[ 0 ];
            TupleWrapper val = (TupleWrapper) sourceCall.getContext()[ 1 ];

            boolean result = sourceCall.getInput().next( key, val );

            if( !result )
                return false;

            sourceCall.getIncomingEntry().setTuple(val.tuple);
            return true;
        }

        @Override
        public void sink(HadoopFlowProcess flowProcess, SinkCall<Object[], OutputCollector> outputCollectorSinkCall) throws IOException {
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
    public TupleEntryIterator openForRead( HadoopFlowProcess flowProcess, RecordReader input ) throws IOException {
        if (input != null)
            return new TupleEntrySchemeIterator( flowProcess, getScheme(), new RecordReaderIterator( input ) );

        JobConf conf = flowProcess.getJobConf();

        return new TupleEntrySchemeIterator(flowProcess, getScheme(),
            new MultiRecordReaderIterator(flowProcess, this, conf), "DBMigrateTap: " + getIdentifier());
    }

    @Override
    public TupleEntryCollector openForWrite(HadoopFlowProcess flowProcess, OutputCollector output) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean createResource(JobConf jc) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(JobConf jc) throws IOException {
        return false;
    }

    @Override
    public boolean resourceExists(JobConf jc) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

}
