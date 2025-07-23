package com.feng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;


public class HBaseTableGenerator {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTableGenerator.class);

    public static class BulkLoadMapper extends Mapper<NullWritable, NullWritable, ImmutableBytesWritable, MapReduceExtendedCell> {
        private static final byte[] CF = Bytes.toBytes("cf");
        private SecureRandom rand;
        private long rowCount;
        private long targetRows;
        private static final Logger logger = LoggerFactory.getLogger(BulkLoadMapper.class);

        @Override
        protected void setup(Context context) {
            rand = new SecureRandom();
            targetRows = context.getConfiguration().getLong("num.rows", 1000000L);
        }

        @Override
        protected void map(NullWritable key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            while (rowCount++ < targetRows) {
                // 生成更高效的RowKey (128位UUID转16字节)
                UUID uuid = UUID.randomUUID();
                byte[] rowKey = new byte[16];
                Bytes.putLong(rowKey, 0, uuid.getMostSignificantBits());
                Bytes.putLong(rowKey, 8, uuid.getLeastSignificantBits());

                for (int col = 1; col <= 20; col++) {
                    byte[] qualifier = Bytes.toBytes("col" + col);
                    byte[] val = new byte[100];
                    rand.nextBytes(val);

                    KeyValue kv = new KeyValue(rowKey, CF, qualifier, val);
                    MapReduceExtendedCell cell = new MapReduceExtendedCell(kv);
                    context.write(new ImmutableBytesWritable(rowKey), cell);
                }

                if (rowCount % 10000 == 0) {
                    logger.info("Generated {} rows", rowCount);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            logger.info("Usage: BigTableGenerator <outputPath> <numRows> <maps>");
            System.exit(1);
        }

        logger.info("Read config file...");
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        conf.setLong("num.rows", Long.parseLong(args[1]));
        conf.setLong("mapreduce.job.maps", Long.parseLong(args[2]));
        conf.set("hbase.mapreduce.hfileoutputformat.compression", "gz");

        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        logger.info("===== [ conf ] ====");
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            logger.info("{}: {}", entry.getKey(), entry.getValue());
        }
        Job job = Job.getInstance(conf, "BigTableDataGen");
        job.setJarByClass(HBaseTableGenerator.class);
        job.setMapperClass(BulkLoadMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setNumReduceTasks(0);

        Path outputPath = new Path(args[0]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(NullInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/tmp/test_input/"));
        try (Connection conn = ConnectionFactory.createConnection(conf);
             RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf("big_table"))) {
            Table table = conn.getTable(TableName.valueOf("big_table"));
            HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}