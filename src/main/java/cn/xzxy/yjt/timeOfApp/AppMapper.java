package cn.xzxy.yjt.timeOfApp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class AppMapper extends Mapper<LongWritable, Text, Text, App> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        App app = new App();
        //获取文件名
        FileSplit fs =(FileSplit) context.getInputSplit();
        String fileName = fs.getPath().getName();
        app.setType(fileName.split("\\.")[0]);
        app.setName(tokens[0]);
        app.setTime(Double.parseDouble(tokens[1]));
        context.write(new Text(app.getName()), app);
    }
}
