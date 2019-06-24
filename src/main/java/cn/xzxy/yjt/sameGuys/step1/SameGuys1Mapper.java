package cn.xzxy.yjt.sameGuys.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SameGuys1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(":");
        String master = tokens[0];
        String[] slaves = tokens[1].split(",");
        for (String slave: slaves) {
            context.write(new Text(slave), new Text(master));
        }
    }
}
