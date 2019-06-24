package cn.xzxy.yjt.profitPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

public class ProfitReducer extends Reducer<IntWritable, Profit, IntWritable, Profit> {

    @Override
    protected void reduce(IntWritable key, Iterable<Profit> values, Context context)
            throws IOException, InterruptedException {
        Profit profit = new Profit();
        for (Profit value: values) {
            profit.setMonth(value.getMonth());
            profit.setName(value.getName());
            profit.setIncome(value.getIncome());
            profit.setTax(value.getTax());
            context.write(key, profit);
        }
    }
}
