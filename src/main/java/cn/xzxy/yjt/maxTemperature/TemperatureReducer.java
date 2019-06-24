package cn.xzxy.yjt.maxTemperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * reduce的组件 继承hadoop.mapreduce.Reducer
 * 参数一、二：输入键类型，输入值类型，同mapper的输出键值类型保持一致
 * 参数三、四：输出键类型，输出值类型
 */
public class TemperatureReducer extends Reducer<Text, Temperature, Text, Temperature> {

    @Override
    protected void reduce(Text key, Iterable<Temperature> values, Context context)
            throws IOException, InterruptedException {

        Temperature t = new Temperature();
        Map<String, String> cityMap = new HashMap<String, String>();
        cityMap.put("1100", "北京");
        cityMap.put("1200", "天津");
        cityMap.put("5000", "重庆");

        int max = Integer.MIN_VALUE;
        for (Temperature value : values) {
            t.setCity(cityMap.get(value.getCity()));
            if (max < value.getTemp()) {
                t.setDate(value.getDate());
                max = value.getTemp();
            }
        }
        t.setTemp(max);
        context.write(key,t);
    }
}
