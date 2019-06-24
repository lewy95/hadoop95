package cn.xzxy.yjt.profitPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProfitPartitioner extends Partitioner<IntWritable, Profit> {


    public int getPartition(IntWritable intWritable, Profit profit, int i) {
        if (profit.getName().equals("zs")){
            return 0;
        }
        if (profit.getName().equals("ls")){
            return 1;
        }
        if (profit.getName().equals("ww")){
            return 2;
        }
        return 0;
    }
}
