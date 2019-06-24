package cn.xzxy.yjt.fullSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FullSortReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable value: values) {
            result += value.get();
        }
        context.write(key, new Text("频次：" + result));
    }
}
