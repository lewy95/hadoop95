package cn.xzxy.yjt.fruitTotal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FruitMapper extends Mapper<LongWritable, Text, Text, Fruit> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] info = line.split(" ");
        Fruit fruit = new Fruit();
        fruit.setName(info[0]);
        fruit.setAddr(info[1]);
        fruit.setWeigh(Integer.parseInt(info[2]));
        fruit.setPrice(Double.parseDouble(info[3]));
        context.write(new Text(fruit.getName()), fruit);
    }
}
