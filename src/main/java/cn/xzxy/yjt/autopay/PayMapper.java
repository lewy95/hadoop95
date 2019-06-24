package cn.xzxy.yjt.autopay;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PayMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(key,value);
    }
}
