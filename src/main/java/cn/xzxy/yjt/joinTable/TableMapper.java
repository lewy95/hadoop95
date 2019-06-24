package cn.xzxy.yjt.joinTable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, Table> {
    private Table v = new Table();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 区分两张表
        FileSplit fs = (FileSplit) context.getInputSplit();
        String name = fs.getPath().getName();

        // 获取一行
        String line = value.toString();

        // 订单表
        if (name.startsWith("order")) {
            String[] tokens = line.split(" ");
            v.setOrder_id(tokens[0]);
            v.setP_id(tokens[1]);
            v.setAmount(Integer.parseInt(tokens[2]));
            v.setPname("");
            v.setFlag("0");

            k.set(tokens[1]);
        } else {// 产品表
            String[] tokens = line.split(" ");
            v.setOrder_id("");
            v.setP_id(tokens[0]);
            v.setAmount(0);
            v.setPname(tokens[1]);
            v.setFlag("1");

            k.set(tokens[0]);
        }

        // 输出
        context.write(k, v);
    }


}
