package cn.xzxy.yjt.sortGoals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GoalsMapper extends Mapper<LongWritable, Text, IntWritable, Goal> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(" ");
        Goal goal = new Goal();
        goal.setName(tokens[0]);
        goal.setGoals(Integer.parseInt(tokens[1]));
        context.write(new IntWritable(1), goal);
    }
}
