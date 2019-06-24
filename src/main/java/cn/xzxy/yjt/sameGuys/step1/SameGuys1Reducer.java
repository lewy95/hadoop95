package cn.xzxy.yjt.sameGuys.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SameGuys1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value).append("-");
        }
        context.write(key,new Text(sb.toString()));
    }
}
