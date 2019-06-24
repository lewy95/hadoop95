package cn.xzxy.yjt.autoIntOut;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PeopleMapper extends Mapper<Text, Text, Text, People> {

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        System.out.println("------mapper");
        People people = new People();
        people.setName(key.toString());
        String[] tokens = value.toString().split("#");
        int age = Integer.parseInt(tokens[0].split(" ")[1]);
        int weight = Integer.parseInt(tokens[1].split(" ")[1]);
        int cost = Integer.parseInt(tokens[2].split(" ")[1]);
        people.setAge(age);
        people.setWeight(weight);
        people.setCost(cost);
        System.out.println("key:" + key + " value:" + people);
        context.write(key, people);
    }
}
