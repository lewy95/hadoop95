package cn.xzxy.yjt.wordCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //将value中的数据求和
        int result = 0;
        for (IntWritable value : values) {
            result += value.get();
        }
        context.write(key, new IntWritable(result));
    }
}
