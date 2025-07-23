package com.feng;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NullInputFormat extends InputFormat<NullWritable, NullWritable> {
    @Override
    public List<InputSplit> getSplits(JobContext context) {
        int numSplits = context.getConfiguration().getInt("mapreduce.job.maps", 1);
        List<InputSplit> splits = new ArrayList<>(numSplits);
        for (int i = 0; i < numSplits; i++) {
            splits.add(new NullInputSplit());
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new NullRecordReader();
    }

    public static class NullInputSplit extends InputSplit implements Writable {
        @Override
        public long getLength() { return 0; }

        @Override
        public String[] getLocations() { return new String[0]; }

        // 实现Writable接口 - 序列化方法
        @Override
        public void write(DataOutput out) throws IOException {
            // 不需要写入任何数据
        }

        // 实现Writable接口 - 反序列化方法
        @Override
        public void readFields(DataInput in) throws IOException {
            // 不需要读取任何数据
        }
    }

    public static class NullRecordReader extends RecordReader<NullWritable, NullWritable> {
        private boolean processed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {}

        @Override
        public boolean nextKeyValue() {
            if (!processed) {
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() { return NullWritable.get(); }

        @Override
        public NullWritable getCurrentValue() { return NullWritable.get(); }

        @Override
        public float getProgress() { return processed ? 1.0f : 0.0f; }

        @Override
        public void close() {}
    }
}
