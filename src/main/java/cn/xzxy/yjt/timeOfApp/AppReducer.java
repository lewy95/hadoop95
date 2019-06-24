package cn.xzxy.yjt.timeOfApp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer extends Reducer<Text, App, Text, App> {

    @Override
    protected void reduce(Text key, Iterable<App> values, Context context)
            throws IOException, InterruptedException {
        App app = new App();
        int count = 0;
        double sum = 0;
        for (App value: values) {
            app.setType(value.getType());
            app.setName(value.getName());
            sum += value.getTime();
            count++;
        }
        app.setTime(sum / count);
        context.write(key, app);
    }
}
