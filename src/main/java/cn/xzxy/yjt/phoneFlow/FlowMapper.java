package cn.xzxy.yjt.phoneFlow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, Flow> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] info = line.split(" ");
        Flow f = new Flow();
        f.setPhone(info[0]);
        f.setAddr(info[1]);
        f.setName(info[2]);
        f.setFlow(Integer.parseInt(info[3]));
        context.write(new Text(f.getPhone()), f);
    }
}
