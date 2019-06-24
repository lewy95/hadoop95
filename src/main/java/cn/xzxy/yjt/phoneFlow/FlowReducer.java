package cn.xzxy.yjt.phoneFlow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce的组件 继承hadoop.mapreduce.Reducer
 * 参数一、二：输入键类型，输入值类型，同mapper的输出键值类型保持一致
 * 参数三、四：输出键类型，输出值类型
 */
public class FlowReducer extends Reducer<Text, Flow, Text, Flow> {

    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context)
            throws IOException, InterruptedException {
        Flow f = new Flow();
        for (Flow value: values ) {
            f.setPhone(value.getPhone());
            f.setAddr(value.getAddr());
            f.setName(value.getName());
            //累加
            f.setFlow(f.getFlow() + value.getFlow());
        }
        context.write(key, f);
    }
}
