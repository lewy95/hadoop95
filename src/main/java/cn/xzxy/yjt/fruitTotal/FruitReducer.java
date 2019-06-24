package cn.xzxy.yjt.fruitTotal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce的组件 继承hadoop.mapreduce.Reducer
 * 参数一、二：输入键类型，输入值类型，同mapper的输出键值类型保持一致
 * 参数三、四：输出键类型，输出值类型
 */
public class FruitReducer extends Reducer<Text, Fruit, Text, Fruit> {

    @Override
    protected void reduce(Text key, Iterable<Fruit> values, Context context)
            throws IOException, InterruptedException {

        Fruit fruit = new Fruit();
        for (Fruit value: values) {
            fruit.setName(value.getName());
            fruit.setAddr(value.getAddr());
            fruit.setWeigh(fruit.getWeigh() + value.getWeigh());
            fruit.setPrice(fruit.getPrice() + value.getPrice());
        }
        //均价
        fruit.setPrice(fruit.getPrice()/fruit.getWeigh());
        context.write(key, fruit);
    }
}
