package cn.xzxy.yjt.autoIntOut;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PeopleReducer extends Reducer<Text, People, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<People> values, Context context)
            throws IOException, InterruptedException {
        System.out.println("------reducer");
        String str = "";
        int cost = 0;
        for (People value: values) {
            str = "age=" + value.getAge() + ",weight=" +value.getWeight() + ",cost=";
            cost += value.getCost();
            System.out.println("每次得到的cost是：" + cost);
        }
        str = str + String.valueOf(cost);
        System.out.println("key:" + key + " value:" + str);
        context.write(key, new Text(str));
    }
}
