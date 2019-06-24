package cn.xzxy.yjt.carSales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CarPartitioner extends Partitioner<Text, Car> {

    public int getPartition(Text text, Car car, int i) {
        if (car.getType().equals("audi")){
            return 0;
        }
        if (car.getType().equals("byd")){
            return 1;
        }
        if (car.getType().equals("saab")){
            return 2;
        }
        return 0;
    }
}
