package cn.xzxy.yjt.sameGuys.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class SameGuys2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /**
         * A F-E-K-O-P-
         * B K-F-A-S-
         * value部分任意两个都是A的朋友，所以F,E的共同好友是A
         */
        String line = value.toString();
        String[] tokens = line.split("\t");
        String slave = tokens[0];
        String[] masters = tokens[1].split("-");

        Arrays.sort(masters);
        for (int i = 0; i < masters.length - 1; i++){
            for (int j = i + 1; j < masters.length; j++){
                context.write(new Text(masters[i] + "-" + masters[j]), new Text(slave));
            }
        }
    }
}
