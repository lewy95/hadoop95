package cn.xzxy.yjt.fullSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FullSortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //将value转换成java的字符串
        String line = value.toString();
        //按照空格将数据切割
        String[] words = line.split(" ");
        //遍历数组，将所有单词作为key，1作为value输出
        int num;
        for (String word : words) {
            num = Integer.parseInt(word);
            context.write(new IntWritable(num), new IntWritable(1));
        }
    }
}
