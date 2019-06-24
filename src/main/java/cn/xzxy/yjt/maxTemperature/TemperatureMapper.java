package cn.xzxy.yjt.maxTemperature;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, Temperature> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        //subString含头不含尾
        String cityId = line.substring(0,4);
        String date = line.substring(5,12);
        int temper = Integer.parseInt(line.substring(12));

        Temperature t = new Temperature();
        t.setCity(cityId);
        t.setDate(date);
        t.setTemp(temper);

        context.write(new Text(t.getCity()), t);
    }
}
