package cn.xzxy.yjt.Distinct;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce的组件 继承hadoop.mapreduce.Reducer
 * 参数一、二：输入键类型，输入值类型，同mapper的输出键值类型保持一致
 * 参数三、四：输出键类型，输出值类型
 */
public class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    /**
     * reduce方法会被调用多次，有几个key会被调用几次
     *
     * @param key  为mapper输出的key
     * @param values  values为输入的value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
