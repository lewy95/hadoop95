package cn.xzxy.yjt.carSales;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CarMapper extends Mapper<LongWritable, Text, Text, Car> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String type = fileSplit.getPath().getName().split("\\.")[0];

        String tokens[] = value.toString().split(" ");
        Car car = new Car();
        car.setType(type);
        car.setName(tokens[1]);
        car.setSale(Integer.parseInt(tokens[2]));

        context.write(new Text(car.getName()), car);
    }
}
