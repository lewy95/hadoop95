package cn.xzxy.yjt.sortGoals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce的组件 继承hadoop.mapreduce.Reducer
 * 参数一、二：输入键类型，输入值类型，同mapper的输出键值类型保持一致
 * 参数三、四：输出键类型，输出值类型
 */
public class GoalReducer extends Reducer<IntWritable, Goal, IntWritable, Goal> {

    @Override
    protected void reduce(IntWritable key, Iterable<Goal> values, Context context)
            throws IOException, InterruptedException {
        int min  = -1;
        Goal goal = new Goal();
        for (Goal value: values) {
            if (min < value.getGoals()){
                goal.setName(value.getName());
                goal.setGoals(value.getGoals());
                min = value.getGoals();
            }
        }
        context.write(key, goal);
    }
}
