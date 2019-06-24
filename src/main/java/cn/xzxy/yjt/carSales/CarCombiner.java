package cn.xzxy.yjt.carSales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarCombiner extends Reducer<Text, Car, Text, Car> {

    @Override
    protected void reduce(Text key, Iterable<Car> values, Context context)
            throws IOException, InterruptedException {
        Car car = new Car();
        for (Car value: values) {
            car.setType(value.getType());
            car.setName(value.getName());
            car.setSale(car.getSale() + value.getSale());
            car.setMonth(car.getMonth() + 1);
        }
        context.write(key, car);
    }
}
