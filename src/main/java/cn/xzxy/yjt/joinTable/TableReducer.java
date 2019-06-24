package cn.xzxy.yjt.joinTable;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, Table, Table, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Table> values, Context context)
            throws IOException, InterruptedException {
        // 1 准备存储订单的集合
        ArrayList<Table> orders = new ArrayList<Table>();
        // 2 准备 bean 对象
        Table pd = new Table();

        for (Table value : values) {
            // 订单表
            if ("0".equals(value.getFlag())) {
                // 拷贝传递过来的每条订单数据到集合中
                Table order = new Table();
                try {
                    BeanUtils.copyProperties(order, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orders.add(order);
            } else {// 产品表
                try {
                    // 拷贝传递过来的产品表到内存中
                    BeanUtils.copyProperties(pd, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 3 表的拼接
        for (Table bean : orders) {
            bean.setPname(pd.getPname());
            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }
    }

}