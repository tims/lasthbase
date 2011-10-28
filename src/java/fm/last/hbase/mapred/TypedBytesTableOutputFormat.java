/**
 * Copyright 2009 Last.fm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.hbase.mapred;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.Progressable;

/**
 * This is a modified version of org.apache.hadoop.hbase.mapred.TableOutputFormat. Converts Map/Reduce typedbytes output
 * and write it to an HBase table
 */
@Deprecated
public class TypedBytesTableOutputFormat extends FileOutputFormat<TypedBytesWritable, TypedBytesWritable> {

  /** JobConf parameter that specifies the output table */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
  private final Log LOG = LogFactory.getLog(TypedBytesTableOutputFormat.class);

  /**
   * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable) and write to an HBase table
   */
  protected static class TableRecordWriter implements RecordWriter<TypedBytesWritable, TypedBytesWritable> {
    private HTable m_table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     *
     * @param table
     */
    public TableRecordWriter(HTable table) {
      m_table = table;
    }

    public void close(Reporter reporter) throws IOException {
      m_table.flushCommits();
    }

    @SuppressWarnings("unchecked")
    public void write(TypedBytesWritable key, TypedBytesWritable value) throws IOException {
      Put put = null;

      if (key == null || value == null) {
        return;
      }
      try {
        put = new Put(key.getBytes());
      } catch (Exception e) {
        throw new IOException("expecting key of type byte[]", e);
      }

      try {
        Map columns = (Map) value.getValue();
        for (Object famObj : columns.keySet()) {
          byte[] family = getBytesFromBuffer((Buffer) famObj);
          Object qualifierCellValueObj = columns.get(family);
          if (qualifierCellValueObj == null) {
            continue;
          }
          Map qualifierCellValue = (Map) qualifierCellValueObj;
          for (Object qualifierObj : qualifierCellValue.keySet()) {
        	byte[] qualifier = getBytesFromBuffer((Buffer) qualifierObj);
            Object cellValueObj = qualifierCellValue.get(qualifier);
            if (cellValueObj == null) {
              continue;
            }
            byte[] cellValue = getBytesFromBuffer((Buffer) cellValueObj);
            put.add(family, qualifier, cellValue);
          }
        }
      } catch (Exception e) {
        throw new IOException("couldn't get column values, expecting Map<Buffer, Map<Buffer, Buffer>>", e);
      }
      m_table.put(put);
    }


    private byte[] getBytesFromBuffer(final Buffer buffer){
	    final int count = buffer.getCount();
	    byte[] bytes = new byte[count];
	    System.arraycopy(buffer.get(), 0, bytes, 0, count);
	    return bytes;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
    throws IOException {

    // expecting exactly one path

    String tableName = job.get(OUTPUT_TABLE);
    HTable table = null;
    try {
      table = new HTable(new HBaseConfiguration(job), tableName);
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    }
    table.setAutoFlush(false);
    return new TableRecordWriter(table);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws FileAlreadyExistsException,
    InvalidJobConfException, IOException {

    String tableName = job.get(OUTPUT_TABLE);
    if (tableName == null) {
      throw new IOException("Must specify table name");
    }
  }
}
