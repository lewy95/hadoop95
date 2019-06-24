package cn.xzxy.yjt.scorePartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, Score> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        Score score = new Score();
        score.setName(tokens[1]);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.equals("chinese.txt")) {
            score.setChinese(Integer.parseInt(tokens[2]));
        } else if (fileName.equals("english.txt")) {
            score.setEnglish(Integer.parseInt(tokens[2]));
        } else {
            score.setMath(Integer.parseInt(tokens[2]));
        }
        context.write(new Text(tokens[1]), score);
    }
}
