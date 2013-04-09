/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

class KEY {
  public static final String INPUT_TABLE = TableInputFormat.INPUT_TABLE;
  public static final String OPTION_CFNUM = "test_key_cfnum";
  public static final String OPTION_COLNUM = "test_key_colnum";
  public static final String CF_PREIX = "test_key_cfprefix";
  public static final String COL_PREIX = "test_key_colprefix";
  public static final String OPTION_COLSFORMAT = "test_key_colsformat";
  public static final String OPTION_USEBULKLOAD = "test_key_usebulkload";
  public static String OPTION_ROWNUM = "test_key_rownum";
  public static String OPTION_BASEROWNUM = "test_key_baserownum";
  public static String OPTION_REGIONROWNUM = "test_key_regionrownum";

  public static long BATCHNUM = 1000;
  public static String BULKLOADDIR = "/bulkload/";
}

class DataGenUtil {
  
  private static Random numberGenerator = new Random(1L);
  
  public static String[] genSequenceStrings(String s, int n) {
    String result[] = new String[n];
    int numberWidth = 0;

    for (int i = 1; i < n; i *= 10) {
      numberWidth++;
    }

    for (int i = 0; i < n; i++) {
      result[i] = Integer.toString(i);
      int padding  = numberWidth - result[i].length();
      for (int j = 0; j < padding; j++) {
        result[i] = "0" + result[i];
      }
      result[i] = s + result[i];
    }

    return result;
  }

  public static String genRandomString(int minLength, int maxLength, String prefix) {
    int lengthOfStr = (int) (Math.random() * (maxLength - minLength + 1))
        + minLength;
    char[] strChar = new char[lengthOfStr];

    for (int i = 0; i < strChar.length; i++) {
      strChar[i] = (char) (Math.random() * 26 + 97);
    }

    String result = new String(strChar);

    if (prefix != null) {
      result = prefix + result;
    }

    return result;
  }

  public static int genRandomInt(int min, int max) { 
    int result = numberGenerator.nextInt(max - min) + min;
    return result;
  }

  public static long genRandomLong() { 
    long result = numberGenerator.nextLong();
    return result;
  }

  public enum ColType {
    NUMINT('i', "numberInt"),
    NUMLONG('l', "numberLong"),
    STRSEQ('s', "stringSequence"),
    TEXTRANDOM('t', "textRandom");

    private String typeName;
    private char id;

    private ColType(char id, String name) {
      this.id = id;
      this.typeName = name;
    }

    public char getID() {
      return this.id;
    }

    public String getColName() {
      return this.typeName;
    }
    
    static public ColType[] parseType(String s) {
      int length = s.length();
      ColType results[] = new ColType[length];
      for (int i = 0; i < length; i++) {
        switch (s.charAt(i)) {
        case 'i':
          results[i] = NUMINT;
          break;
        case 'l':
          results[i] = NUMLONG;
          break;
        case 's':
          results[i] = STRSEQ;
          break;
        case 't':
          results[i] = TEXTRANDOM;
          break;
        default:
          results[i] = STRSEQ;
          break;
        }
      }
      return results;
    }
  }

  public class ColFormat {
    public ColType type;
    
    public ColFormat(ColType t) {
      this.type = t;
    }
  }
  
  public ColFormat[] parseColsFormat(String formatStr) {
    String[] cols = formatStr.split(",");
    int length = cols.length;
    ColFormat results[] = new ColFormat[length];
    ColFormat curCol = null;
    String[] fields = null;

    for (int i = 0 ; i < length; i++) {
      fields = cols[i].split(":");
      if (fields[0].equalsIgnoreCase("i")) {
        if (fields.length < 2 || fields.length > 3)
          throw new RuntimeException("parseColsFormat Err");
        int minint = Integer.parseInt(fields[1]);
        int maxint = minint;
        if (fields.length > 2)
          maxint = Integer.parseInt(fields[2]);
        if (maxint < minint)
          maxint = minint;
        curCol = new ColIntRange(minint, maxint);
        results[i] = curCol;
      } else if(fields[0].equalsIgnoreCase("l")) {
        curCol = new ColLong();
        results[i] = curCol;
      } else if(fields[0].equalsIgnoreCase("s")){
        curCol = new ColStrSeq();
        results[i] = curCol;
      } else if(fields[0].equalsIgnoreCase("t")){
        if (fields.length < 2 || fields.length > 3)
          throw new RuntimeException("parseColsFormat Err");

        int minlength = Integer.parseInt(fields[1]);
        int maxlength = 0;
        if (fields.length > 2)
          maxlength = Integer.parseInt(fields[2]);
        if (maxlength < minlength)
          maxlength = minlength;
        curCol = new ColTextRandom(minlength,maxlength);
        results[i] = curCol;
      } else {
        throw new RuntimeException("parseColsFormat Err");
      }
    }
    return results;
  }
  
  public class ColTextRandom extends ColFormat {
    public int minLength;
    public int maxLength;
    
    public ColTextRandom(int min, int max) {
      super(ColType.TEXTRANDOM);
      minLength = min;
      maxLength = max;
    }
  }

  public class ColIntRange extends ColFormat {
    public int min;
    public int max;
    public ColIntRange(int min, int max) {
      super(ColType.NUMINT);
      this.min = min;
      this.max = max;
    }
  }
  
  public class ColLong extends ColFormat {
    public ColLong() {
      super(ColType.NUMLONG);
    }
  }
  
  public class ColStrSeq extends ColFormat {
    public ColStrSeq() {
      super(ColType.STRSEQ);
    }
  }
}

class RegionWriteInputFormat extends
    InputFormat<String, Long> {

  public static class RegionWriteRecordReader extends
      RecordReader<String, Long> {
    private TableSplit value = null;
    private Configuration conf;
    private long rowNum;
    private long index = 0L;
    private long baseRowNumber = 1L;
    private String regionPrefix = null;
    private boolean currentValueRead = false;

    public RegionWriteRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      initialize(split, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      this.value = (TableSplit) split;
      conf = context.getConfiguration();
      this.rowNum = conf.getLong(KEY.OPTION_REGIONROWNUM, 100000L);
      this.baseRowNumber = conf.getLong(KEY.OPTION_BASEROWNUM , 1L);
      this.index = 0L;
      
      byte[] srow = value.getStartRow();
      if (srow.length == 0) {
        // this is the first region, we use "aaaa" for prefix.
        regionPrefix = "aaaa";        
      } else {
        regionPrefix = Bytes.toString(srow);
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (index >= rowNum) {
        return false;
      }
      if (currentValueRead == true) {
        index += KEY.BATCHNUM;
        currentValueRead = false;
      }
      return true;
    }

    @Override
    public String getCurrentKey() throws IOException,
        InterruptedException {
      String s = regionPrefix + Long.toString((this.baseRowNumber + index) / KEY.BATCHNUM);
      return s;
    }

    @Override
    public Long getCurrentValue() throws IOException,
        InterruptedException {
      currentValueRead = true;
      if ((rowNum - index) > KEY.BATCHNUM)
        return KEY.BATCHNUM;
      return rowNum - index;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return index * 1.0f / rowNum;
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    TableInputFormat tif = new TableInputFormat();
    tif.setConf(context.getConfiguration());
    return tif.getSplits(context);
  }

  @Override
  public RecordReader<String, Long> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RegionWriteRecordReader(split, context);
  }
}

class GenerateRegionDataTask extends
    Mapper<String, Long, LongWritable, LongWritable> {

  private int colNum;
  private int cfNum = 1;
  private String tableName = null;
  private String cfPrefix = null;
  private String colPrefix = null;
  private HTable ht = null;
  private String[] families = null;
  private String[] columns = null;

  private Configuration conf;
  private DataGenUtil.ColFormat[] ColsFormat;
  private BulkWriter[] bulkWriters;

  private boolean useBulkload = false;

  public class BulkWriter {
    StoreFile.Writer writer = null;
  }

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {

    conf = context.getConfiguration();

    this.cfNum = conf.getInt(KEY.OPTION_CFNUM, 1);
    this.colNum = conf.getInt(KEY.OPTION_COLNUM, 18);
    this.tableName = conf.get(KEY.INPUT_TABLE);
    this.cfPrefix = conf.get(KEY.CF_PREIX, "F");
    this.colPrefix = conf.get(KEY.COL_PREIX, "C");

    families = DataGenUtil.genSequenceStrings(cfPrefix, this.cfNum);
    columns = DataGenUtil.genSequenceStrings(colPrefix, this.colNum);
    
    this.useBulkload  = conf.getBoolean(KEY.OPTION_USEBULKLOAD, false);

    String colsFormatString = conf.get(KEY.OPTION_COLSFORMAT, "");
    
    DataGenUtil dataGenUtil = new DataGenUtil();
    this.ColsFormat = dataGenUtil.parseColsFormat(colsFormatString);

    try {
      ht = new HTable(conf, tableName);
    } catch (IOException e) {
      assertNull("Failed to create table", e);
    }

    if (this.useBulkload) {
      bulkWriters = new BulkWriter[this.cfNum];
      Path bulkOutputPath = new Path(KEY.BULKLOADDIR + tableName);
      FileSystem fs = bulkOutputPath.getFileSystem(conf);
      
      for (int i = 0; i < families.length; i++) {
        String family = families[i];
        Path cfPath = new Path(bulkOutputPath, family);
        
        if (!fs.exists(cfPath)) {
          fs.mkdirs(cfPath);
        }

        HColumnDescriptor cfDesc = ht.getTableDescriptor().getFamily(family.getBytes());
        HFileDataBlockEncoder dataBlockEncoder = new HFileDataBlockEncoderImpl(
            cfDesc.getDataBlockEncodingOnDisk(),
            cfDesc.getDataBlockEncoding());
        
        Configuration tempConf = new Configuration(conf);

        BulkWriter w = new BulkWriter();

        w.writer = new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf),
            fs, cfDesc.getBlocksize())
            .withOutputDir(cfPath)
            .withCompression(cfDesc.getCompression())
            .withBloomType(cfDesc.getBloomFilterType())
            .withComparator(KeyValue.COMPARATOR)
            .withDataBlockEncoder(dataBlockEncoder)
            .withChecksumType(Store.getChecksumType(conf))
            .withBytesPerChecksum(Store.getBytesPerChecksum(conf))
            .build();
        
        bulkWriters[i] = w;
      }
    }
    
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {

    if (ht != null)
      ht.close();

    if (this.useBulkload) {
      for (BulkWriter w : this.bulkWriters) {
        w.writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
            Bytes.toBytes(System.currentTimeMillis()));
        w.writer.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
            Bytes.toBytes(context.getTaskAttemptID().toString()));
        w.writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
            Bytes.toBytes(true));
        w.writer.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
            Bytes.toBytes(false));
        w.writer.appendTrackedTimestampsToMetadata();
        w.writer.close();
      }
    }
  }

  @Override
  protected void map(String prefix, Long rows, final Context context)
      throws IOException, InterruptedException {

    //long startTime = System.currentTimeMillis();
    // region write task
    try {
        doWrite(prefix, rows, context);        
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }

    //long elapsedTime = System.currentTimeMillis() - startTime;

    context.progress();
  }
  
  private String longToStringPadding(int width, long val) {
    String result = Long.toString(val);
    int padding  = width - result.length();

    for (int i = 0; i < padding; i++) {
      result = "0" + result;
    }
    return result;
  }
  
  private void doWrite(String rowPrefix, Long rows, final Context context) throws IOException {

    long remainRows = rows;
    long index = 0;
    int toProcess;
    String row = null;
    Put p = null;
    BulkWriter w = null;

    long ts = this.useBulkload ? System.currentTimeMillis() : HConstants.LATEST_TIMESTAMP;

    int rowWidth = 0;
    for (long i = 1L; i < rows; i *= 10L) {
      rowWidth++;
    }

    while (remainRows > 0) {
      toProcess = (int) KEY.BATCHNUM;
      if (toProcess > remainRows)
        toProcess = (int) remainRows;

      List<Put> putList = new ArrayList<Put>(toProcess);

      for (int i = 0; i < toProcess; i++) {
        row = rowPrefix + longToStringPadding(rowWidth, index);
        if (!this.useBulkload) {
          p = new Put(Bytes.toBytes(row));
          p.setWriteToWAL(false);
        }

        for (int fIndex = 0; fIndex < families.length; fIndex++) {
          String family = families[fIndex];

          if (this.useBulkload) {
            w = this.bulkWriters[fIndex];
          }

          for (int cIndex = 0; cIndex < columns.length; cIndex++) {
            String column = columns[cIndex];
            KeyValue kv;
            switch (this.ColsFormat[cIndex].type) {
            case NUMINT:
              kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
                  Bytes.toBytes(column), ts,
                  KeyValue.Type.Put,
                  Bytes.toBytes(DataGenUtil.genRandomInt(
                      ((DataGenUtil.ColIntRange)this.ColsFormat[cIndex]).min,
                      ((DataGenUtil.ColIntRange)this.ColsFormat[cIndex]).max
                      )));
              break;
            case NUMLONG:
              kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
                  Bytes.toBytes(column), ts,
                  KeyValue.Type.Put,
                  Bytes.toBytes(DataGenUtil.genRandomLong()));
              break;
            case STRSEQ:
              kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
                  Bytes.toBytes(column), ts,
                  KeyValue.Type.Put,
                  Bytes.toBytes("v" + "-" + column + "-" + row));
              break;
            case TEXTRANDOM:
              kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
                  Bytes.toBytes(column), ts,
                  KeyValue.Type.Put,
                  Bytes.toBytes(DataGenUtil.genRandomString(
                      ((DataGenUtil.ColTextRandom)this.ColsFormat[cIndex]).minLength,
                      ((DataGenUtil.ColTextRandom)this.ColsFormat[cIndex]).maxLength, null)));
              break;
            default:
              kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
                  Bytes.toBytes(column), ts,
                  KeyValue.Type.Put,
                  Bytes.toBytes("v" + "-" + column + "-" + row));
              break;
            }

            //KeyValue kv = KeyValueTestUtil.create(row, family, column,
            //    ts, "v" + "-" + column + "-" + row);
            
            if (!this.useBulkload) {
              p.add(kv);
            } else {
              w.writer.append(kv);
            }
          }
        }

        if (!this.useBulkload) {
          putList.add(p);
        }
        index++;
      }

      if (!this.useBulkload) {
        ht.put(putList);
      }
      remainRows -= toProcess;
    }
  }
}


public class GenerateTestTable {

  static final Log LOG = LogFactory.getLog(GenerateTestTable.class);

  private HBaseAdmin admin = null;

  private final int MAX_COLUMN_NUM = 100;
  private final int DEFAULT_COLUMN_NUM = 5;
  private final int MAX_FAMILY_NUM = 10;
  private final int DEFAULT_FAMILY_NUM = 1;
  private final long MAX_ROW_NUM = 10000000000L;
  private final long DEFAULT_ROW_NUM = 10000;
  private final int MAX_REGION_NUM = 2048;
  private final int DEFAULT_REGION_NUMBER = 96;

  private final String[] rowPrefix = {
      "a","b","c","d","e","f","g","h","i","j","k","l","m",
      "n","o","p","q","r","s","t","u","v","w","x","y","z"
  };
  private String cfPrefix = "F";
  private String colPrefix = "C";
  private String dotColPrefix = "D.C";

  private Configuration conf = null;
  private byte[][] tableSplits = null;
  
  private int colNum = this.DEFAULT_COLUMN_NUM;
  private String coltypes = null;
  private long rowNum = this.DEFAULT_ROW_NUM;
  private int cfNum = this.DEFAULT_FAMILY_NUM;
  private int regionNumber = this.DEFAULT_REGION_NUMBER;
  private long baseRowNumber = 1L;
  private String tableName = null;
  private boolean createDotTable = false;
  private int maxVersions = 1;
  private String dotTableName;

  private Compression.Algorithm compression = Compression.Algorithm.NONE;
  private DataBlockEncoding encoding = DataBlockEncoding.NONE;

  private boolean useBulkLoad = false;


  /**
   * Constructor
   * 
   * @param c
   *          Configuration object
   */
  public GenerateTestTable() {

  }
  
  private void init(){
    this.conf = HBaseConfiguration.create();
    try {
      this.admin = new HBaseAdmin(this.conf);
    } catch (MasterNotRunningException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void doMapReduce(
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass,
      String mrTableName, String cfPrefix, String colPrefix) throws IOException,
      ClassNotFoundException, InterruptedException {

    this.conf.set(KEY.INPUT_TABLE, mrTableName);
    Job job = new Job(this.conf);
    job.setJobName("Generate Data for [" + mrTableName + "]");
    job.setJarByClass(GenerateTestTable.class);

    this.conf.set(KEY.CF_PREIX, cfPrefix);
    this.conf.set(KEY.COL_PREIX, colPrefix);

    job.setInputFormatClass(inputFormatClass);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path("/tmp", "tempout");
    fs.delete(path, true);

    FileOutputFormat.setOutputPath(job, path);

    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);

    TableMapReduceUtil.addDependencyJars(job);
    // Add a Class from the hbase.jar so it gets registered too.
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.util.Bytes.class);

    TableMapReduceUtil.initCredentials(job);

    job.waitForCompletion(true);

  }
  
  private void createNormalTable(String tableName,
      Map<String, String[]> layouts, byte[][] splits) {

    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (Map.Entry<String, String[]> cfLayout : layouts.entrySet()) {
      String family = cfLayout.getKey();
      HColumnDescriptor cfdesc = new HColumnDescriptor(family)
          .setMaxVersions(maxVersions)
          .setDataBlockEncoding(encoding)
          .setCompressionType(compression);
      htd.addFamily(cfdesc);
    }

    try {
      if (splits == null) {
        admin.createTable(htd);
      } else {
        admin.createTable(htd, splits);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void createDotTable(String tableName,
      Map<String, String[]> layouts, byte[][] splits) {

    HTableDescriptor htd = new HTableDescriptor(tableName);

    for (Map.Entry<String, String[]> cfLayout : layouts.entrySet()) {
      String family = cfLayout.getKey();
      String[] columns = cfLayout.getValue();

      HColumnDescriptor cfdesc = new HColumnDescriptor(family)
          .setMaxVersions(maxVersions)
          .setDataBlockEncoding(encoding)
          .setCompressionType(compression);

      Map<String, List<String>> docsMap = new HashMap<String, List<String>>();

      for (String q : columns) {
        int idx = q.indexOf(".");
        String doc = q.substring(0, idx);
        String field = q.substring(idx + 1);

        List<String> fieldList = docsMap.get(doc);

        if (fieldList == null) {
          fieldList = new ArrayList<String>();
          docsMap.put(doc, fieldList);
        }

        fieldList.add(field);
      }

      String[] docs = new String[docsMap.entrySet().size()];
      int index = 0;

      for (Map.Entry<String, List<String>> m : docsMap.entrySet()) {
        String docName = m.getKey();
        List<String> fields = m.getValue();
        boolean firstField = true;

        docs[index++] = docName;

        String docSchemaId = "hbase.dot.columnfamily.doc.schema." + docName;
        String docSchemaValue = " {    \n" + " \"name\": \"" + docName
            + "\", \n" + " \"type\": \"record\",\n" + " \"fields\": [\n";
        for (String field : fields) {
          if (firstField) {
            firstField = false;
          } else {
            docSchemaValue += ", \n";
          }
          docSchemaValue += " {\"name\": \"" + field
              + "\", \"type\": \"bytes\"}";
        }

        docSchemaValue += " ]}";
        LOG.info("--- " + family + ":" + docName + " = " + docSchemaValue);
        cfdesc.setValue(docSchemaId, docSchemaValue);
      }
      String docElements = StringUtils.arrayToString(docs);
      cfdesc.setValue("hbase.dot.columnfamily.doc.element", docElements);
      htd.addFamily(cfdesc);
    }

    htd.setValue("hbase.dot.enable", "true");
    htd.setValue("hbase.dot.type", "ANALYTICAL");

    try {
      if (splits == null) {
        admin.createTable(htd);
      } else {
        admin.createTable(htd, splits);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void doBulkLoad() throws Exception {
    
    HTable ht = new HTable(conf, this.tableName);
    new LoadIncrementalHFiles(conf).doBulkLoad(new Path(KEY.BULKLOADDIR + tableName), ht);
    ht.close();
    // We don't support bulkload for dot yet.
  }

  public void doMajorCompact() throws Exception {
    admin.flush(this.tableName);
    if (this.createDotTable)
      admin.flush(this.dotTableName);

    admin.majorCompact(this.tableName);
    if (this.createDotTable)
      admin.majorCompact(this.dotTableName);
  }
  
  public void createTable() throws Exception {

    String familys[] = DataGenUtil.genSequenceStrings(cfPrefix, this.cfNum);
    String columns[] = DataGenUtil.genSequenceStrings(colPrefix, this.colNum);

    Map<String, String[]> layouts = new HashMap<String, String[]>();
    
    for (String family : familys) {
      layouts.put(family, columns);
    }
    
    tableSplits = getFourLetterSplits(this.regionNumber);
    createNormalTable(tableName, layouts, tableSplits);

    if (this.createDotTable) {
      Map<String, String[]> dotLayouts = new HashMap<String, String[]>();
      String[] dotColumns = DataGenUtil.genSequenceStrings(dotColPrefix, this.colNum);
      for (String family : familys) {
        dotLayouts.put(family, dotColumns);
      }
      this.dotTableName = this.tableName + "Dot";
      createDotTable(this.dotTableName, dotLayouts, tableSplits);
    }
  }

  protected void printUsage() {
    printUsage(null);
  }

  protected void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName());
    System.err.println("--table=tablename [--rownum=] [--colnum=] [--cfnum=] [--regions=] [--enabledot]");
    System.err.println("[--colsformat=i:min:max(int),|l(long),|s(sequence string),|t:minlengh:maxlength(random text),]");
    System.err.println("[--encoding=prefix|diff|fastdiff|none] [--compression=gz|lzo|snappy|none]");
    System.err.println("[--usebulkload]");
    System.err.println();
  }

  private byte[][] getFourLetterSplits(int n) {
    double range = 26.0 * 26.0 * 26.0 * 26.0;
    assert(n > 0 && n < MAX_REGION_NUM);
    byte[][] splits = new byte[n-1][];

    double step = range / n;
    double offset = 0.0;
    long index;
    char[] letter = new char[4];
    for (int i = 0; i < (n-1); i++) {
      offset += step;
      index = Math.round(offset);
      letter[0] = (char) ((index / (26*26*26)) + 97);
      letter[1] = (char) ((index / (26*26) % 26) + 97);
      letter[2] = (char) ((index / (26) % 26) + 97);
      letter[3] = (char) ((index % 26) + 97);
      splits[i] = Bytes.toBytes(new String(letter));
    }
    return splits;
  }

  public int parseCommandLine(final String[] args) {
    // (but hopefully something not as painful as cli options).
    int errCode = 0;
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-h") || cmd.startsWith("--h")) {
        printUsage();
        break;
      }

      final String colnum = "--colnum=";
      if (cmd.startsWith(colnum)) {
        int val = Integer.parseInt(cmd.substring(colnum.length()));
        if (val <= 0 || val > this.MAX_COLUMN_NUM)
          val = this.DEFAULT_COLUMN_NUM;
        this.colNum = val;
        this.conf.setLong(KEY.OPTION_COLNUM, this.colNum);
        continue;
      }

      final String cf = "--cfnum=";
      if (cmd.startsWith(cf)) {
        int val = Integer.parseInt(cmd.substring(cf.length()));
        if (val <= 0 || val > this.MAX_FAMILY_NUM)
          val = this.DEFAULT_FAMILY_NUM;
        this.cfNum = val;
        this.conf.setInt(KEY.OPTION_CFNUM, this.cfNum);
        continue;
      }
      
      final String rows = "--rownum=";
      if (cmd.startsWith(rows)) {
        long val = Long.decode(cmd.substring(rows.length()));
        if (val < 0 || val > this.MAX_ROW_NUM)
          val = this.DEFAULT_ROW_NUM;
        this.rowNum = val;
        continue;
      }

      final String colsFormat = "--colsformat=";
      if (cmd.startsWith(colsFormat)) {
        this.coltypes  = cmd.substring(colsFormat.length());
        continue;
      }

      final String regions = "--regions=";
      if (cmd.startsWith(regions)) {
        int val = Integer.parseInt(cmd.substring(regions.length()));
        if (val <= 0 || val > this.MAX_REGION_NUM)
          val = this.DEFAULT_REGION_NUMBER;
        this.regionNumber = val;    
        continue;
      }
      
      final String enabledot = "--enabledot";
      if (cmd.startsWith(enabledot)) {
        this.createDotTable  = true;    
        continue;
      }

      final String usebulkload = "--usebulkload";
      if (cmd.startsWith(usebulkload)) {
        this.useBulkLoad  = true;    
        continue;
      }

      final String compressionOP = "--compression=";
      if (cmd.startsWith(compressionOP)) {
        String compressionCodec = cmd.substring(compressionOP.length());
        if (compressionCodec.equalsIgnoreCase("gz")) {
          this.compression = Compression.Algorithm.GZ;
        } else if (compressionCodec.equalsIgnoreCase("lzo")) {
          this.compression = Compression.Algorithm.LZO;
        } else if (compressionCodec.equalsIgnoreCase("snappy")) {
          this.compression = Compression.Algorithm.SNAPPY;
        } else {
          this.compression = Compression.Algorithm.NONE;
        }
        continue;
      }

      final String encodingOP = "--encoding=";
      if (cmd.startsWith(encodingOP)) {
        String encodingCodec = cmd.substring(encodingOP.length());
        if (encodingCodec.equalsIgnoreCase("fastdiff")) {
          this.encoding = DataBlockEncoding.FAST_DIFF;
        } else if (encodingCodec.equalsIgnoreCase("diff")) {
          this.encoding = DataBlockEncoding.DIFF;
        } else if (encodingCodec.equalsIgnoreCase("prefix")) {
          this.encoding = DataBlockEncoding.PREFIX;
        } else {
          this.encoding = DataBlockEncoding.NONE;
        }
        continue;
      }

      final String table = "--table=";
      if (cmd.startsWith(table)) {
        this.tableName = cmd.substring(table.length());
        continue;
      }
    }

    if (this.tableName == null) {
        printUsage("Please specify the table name");
        errCode = -2;
    }

    if (this.coltypes == null) {
      this.coltypes = "s";
      for (int j = 1; j < this.colNum; j++) {
        this.coltypes = this.coltypes.concat(",s");
      }
    }
    
    DataGenUtil dataGenUtil = new DataGenUtil();
    DataGenUtil.ColFormat[] colsFormat = dataGenUtil.parseColsFormat(this.coltypes);
    if (colsFormat.length != this.colNum) {
      System.err.println("colformat string : " + this.coltypes + " does not match colNum");
      errCode = -3;
    }

    this.conf.set(KEY.OPTION_COLSFORMAT, this.coltypes);

    this.baseRowNumber = 1L;
    while(this.baseRowNumber < this.rowNum) {
      this.baseRowNumber *= 10L;
    }

    this.conf.setLong(KEY.OPTION_BASEROWNUM, this.baseRowNumber);
    this.conf.setLong(KEY.OPTION_REGIONROWNUM , this.rowNum / this.regionNumber);

    this.conf.setBoolean(KEY.OPTION_USEBULKLOAD, useBulkLoad);

    System.out.println("cfnum = " + this.cfNum);
    System.out.println("colnum = " + this.colNum);
    System.out.println("colsFormat = " + colsFormat);
    System.out.println("rownum = " + this.rowNum);
    System.out.println("baseRowNumber = " + this.baseRowNumber);
    System.out.println("tablename = " + this.tableName);
    System.out.println("Presplit Region number = " + this.regionNumber);
    System.out.println("row per region = " + this.rowNum / this.regionNumber);
    System.out.println("Also create dot table = " + this.createDotTable);
    System.out.println("Data Block Encoding = " + this.encoding);
    System.out.println("Compression = " + this.compression);
    System.out.println("Use BulkLoad = " + this.useBulkLoad);
    return errCode;
  }

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(final String[] args) throws Exception {
    GenerateTestTable gt = new GenerateTestTable();
    gt.init();
    if (gt.parseCommandLine(args) != 0) {
      System.err.println("fail to parse cmdline");
      return;
    }
    gt.createTable();
    
    if (gt.rowNum == 0) {
      System.out.println("rowNum=0, only create table");
      return;
    }

    gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.tableName, gt.cfPrefix, gt.colPrefix);
    
    if (gt.createDotTable) {
      gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.dotTableName, gt.cfPrefix, gt.dotColPrefix);
    }

    if (gt.useBulkLoad) {
      System.out.println("######## bulkloading table... ###########");
      gt.doBulkLoad();
    } else {
      System.out.println("######## Major compacting table... ###########");
      gt.doMajorCompact();
    }
  }
  
  /**
   * for test usage.
   * @param args
   * @throws Exception
   */
  public static void testmain(final String[] args,Configuration conf) throws Exception {
    GenerateTestTable gt = new GenerateTestTable();
    gt.conf = conf;
    gt.admin = new HBaseAdmin(gt.conf);
    if (gt.parseCommandLine(args) != 0) {
      System.err.println("fail to parse cmdline");
      return;
    }
    gt.createTable();
    
    if (gt.rowNum == 0) {
      System.out.println("rowNum=0, only create table");
      return;
    }

    gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.tableName, gt.cfPrefix, gt.colPrefix);
    
    if (gt.createDotTable) {
      gt.doMapReduce(RegionWriteInputFormat.class,
          GenerateRegionDataTask.class,
          gt.dotTableName, gt.cfPrefix, gt.colPrefix);
    }
    
    if (gt.useBulkLoad) {
      System.out.println("######## bulkloading table... ###########");
      gt.doBulkLoad();
    } else {
      System.out.println("######## Major compacting table... ###########");
      gt.doMajorCompact();
    }
  }
}
