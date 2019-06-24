package cn.xzxy.yjt.fullSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FullSortPartitioner extends Partitioner<IntWritable,IntWritable> {

    public int getPartition(IntWritable num, IntWritable count, int i) {
        //将IntWritable转化为int型
        int figure = num.get();
        if (figure < 100) {
            return 0;
        }
        if (figure >= 100 && figure < 1000) {
            return 1;
        }
        if (figure >= 1000) {
            return 2;
        }
        return 0;
    }
}
