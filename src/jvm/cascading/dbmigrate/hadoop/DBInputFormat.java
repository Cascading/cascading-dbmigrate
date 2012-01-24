/**
 Copyright 2010 BackType

 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package cascading.dbmigrate.hadoop;

import cascading.tuple.Tuple;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

public class DBInputFormat implements InputFormat<LongWritable, TupleWrapper> {

    private static final Logger LOG = LoggerFactory.getLogger(DBInputFormat.class);

    public static class DBRecordReader implements RecordReader<LongWritable, TupleWrapper> {

        private ResultSet results;
        private Statement statement;
        private Connection connection;
        private DBInputSplit split;
        private long pos = 0;

        protected DBRecordReader(DBInputSplit split, JobConf job) throws IOException {
            try {
                this.split = split;
                DBConfiguration conf = new DBConfiguration(job);
                connection = conf.getConnection();

                statement = connection
                    .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                //statement.setFetchSize(Integer.MIN_VALUE);
                String query = getSelectQuery(conf, split);
                LOG.info("Running query: " + query);
                try {
                    results = statement.executeQuery(query);
                } catch (SQLException exception) {
                    LOG.error("unable to execute select query: " + query, exception);
                    throw new IOException("unable to execute select query: " + query, exception);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public static <T> String join(T[] arr, String sep) {
            String ret = "";
            for (int i = 0; i < arr.length; i++) {
                ret = ret + arr[i];
                if (i < arr.length - 1) {
                    ret = ret + sep;
                }
            }
            return ret;
        }

        protected String getSelectQuery(DBConfiguration conf, DBInputSplit split) {
            StringBuilder query = new StringBuilder();
            query.append("SELECT ");
            query.append(join(conf.getInputColumnNames(), ","));
            query.append(" FROM ");
            query.append(conf.getInputTableName());
            query.append(" WHERE ");
            query.append(split.primaryKeyColumn + ">=" + split.startId);
            query.append(" AND ");
            query.append(split.primaryKeyColumn + "<" + split.endId);
            return query.toString();
        }

        public void close() throws IOException {
            try {
                results.close();
                statement.close();
                connection.close();
            } catch (SQLException exception) {
                throw new IOException("unable to commit and close", exception);
            }
        }

        /** {@inheritDoc} */
        public LongWritable createKey() {
            return new LongWritable();
        }

        /** {@inheritDoc} */
        public TupleWrapper createValue() {
            return new TupleWrapper();
        }

        /** {@inheritDoc} */
        public long getPos() throws IOException {
            return pos;
        }

        /** {@inheritDoc} */
        public float getProgress() throws IOException {
            return (pos / (float) split.getLength());
        }

        /** {@inheritDoc} */
        public boolean next(LongWritable key, TupleWrapper value) throws IOException {
            try {
                if (!results.next()) {
                    return false;
                }
                key.set(pos + split.startId);

                value.tuple = new Tuple();

                for (int i = 0; i < results.getMetaData().getColumnCount(); i++) {
                    Object o = results.getObject(i + 1);
                    if (o instanceof byte[]) {
                        o = new BytesWritable((byte[]) o);
                    } else if (o instanceof BigInteger) {
                        o = ((BigInteger) o).longValue();
                    } else if (o instanceof BigDecimal) {
                        o = ((BigDecimal) o).doubleValue();
                    }
                    try {
                        value.tuple.add(o);
                    } catch (Throwable t) {
                        LOG.info("WTF: " + o.toString() + o.getClass().toString());
                        throw new RuntimeException(t);
                    }
                }
                pos++;
            } catch (SQLException exception) {
                throw new IOException("unable to get next value", exception);
            }
            return true;
        }
    }

    protected static class DBInputSplit implements InputSplit {

        public long endId = 0;
        public long startId = 0;
        public String primaryKeyColumn;

        public DBInputSplit() {
        }

        public DBInputSplit(long start, long end, String primaryKeyColumn) {
            startId = start;
            endId = end;
            this.primaryKeyColumn = primaryKeyColumn;
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public long getLength() throws IOException {
            return endId - startId;
        }

        public void readFields(DataInput input) throws IOException {
            startId = input.readLong();
            endId = input.readLong();
            primaryKeyColumn = WritableUtils.readString(input);
        }

        public void write(DataOutput output) throws IOException {
            output.writeLong(startId);
            output.writeLong(endId);
            WritableUtils.writeString(output, primaryKeyColumn);
        }
    }

    public RecordReader<LongWritable, TupleWrapper> getRecordReader(InputSplit split, JobConf job,
        Reporter reporter) throws IOException {
        return new DBRecordReader((DBInputSplit) split, job);
    }

    private long getMaxId(DBConfiguration conf, Connection conn, String tableName, String col) {
        if (conf.getMaxId() != null) {
            return conf.getMaxId();
        }
        try {
            PreparedStatement s =
                conn.prepareStatement("SELECT MAX(" + col + ") FROM " + tableName);
            ResultSet rs = s.executeQuery();
            rs.next();
            long ret = rs.getLong(1);
            rs.close();
            s.close();
            return ret;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long getMinId(DBConfiguration conf, Connection conn, String tableName, String col) {
        if (conf.getMinId() != null) {
            return conf.getMinId();
        }
        try {
            PreparedStatement s =
                conn.prepareStatement("SELECT MIN(" + col + ") FROM " + tableName);
            ResultSet rs = s.executeQuery();
            rs.next();
            long ret = rs.getLong(1);
            rs.close();
            s.close();
            return ret;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public InputSplit[] getSplits(JobConf job, int ignored) throws IOException {
        try {
            DBConfiguration conf = new DBConfiguration(job);
            int chunks = conf.getNumChunks();
            Connection conn = conf.getConnection();
            String primarykeycolumn = conf.getPrimaryKeyColumn();
            long maxId = getMaxId(conf, conn, conf.getInputTableName(), conf.getPrimaryKeyColumn());
            long minId = getMinId(conf, conn, conf.getInputTableName(), conf.getPrimaryKeyColumn());
            long chunkSize = (maxId - minId + 1) / chunks + 1;
            chunks = (int) ((maxId - minId + 1) / chunkSize) + 1;
            InputSplit[] ret = new InputSplit[chunks];

            long currId = minId;
            for (int i = 0; i < chunks; i++) {
                long start = currId;
                currId += chunkSize;
                ret[i] = new DBInputSplit(start, Math.min(currId, maxId + 1), primarykeycolumn);
            }

            conn.close();
            return ret;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setInput(JobConf job, int numChunks, String databaseDriver, String username,
        String pwd, String dburl, String tableName, String pkColumn, Long minId, Long maxId,
        String... columnNames) {
        job.setInputFormat(DBInputFormat.class);

        DBConfiguration dbConf = new DBConfiguration(job);
        dbConf.configureDB(databaseDriver, dburl, username, pwd);
        if (minId != null) {
            dbConf.setMinId(minId.longValue());
        }
        if (maxId != null) {
            dbConf.setMaxId(maxId.longValue());
        }
        dbConf.setInputTableName(tableName);
        dbConf.setInputColumnNames(columnNames);
        dbConf.setPrimaryKeyColumn(pkColumn);
        dbConf.setNumChunks(numChunks);
    }
}
