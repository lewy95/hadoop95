package cn.xzxy.yjt.profitPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProfitMapper extends Mapper<LongWritable, Text, IntWritable, Profit> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String token[] = value.toString().split(" ");
        Profit profit = new Profit();
        profit.setMonth(token[0]);
        profit.setName(token[1]);
        profit.setIncome(Integer.parseInt(token[2]));
        profit.setTax(Integer.parseInt(token[3]));
        context.write(new IntWritable(profit.getIncome() - profit.getTax()), profit);
    }
}
