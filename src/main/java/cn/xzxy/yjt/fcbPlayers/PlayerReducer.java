package cn.xzxy.yjt.fcbPlayers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PlayerReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        System.out.println("------reducer");
        String str = "";
        int seasonCount = 0;
        int goals = 0;
        int assists = 0;
        for (Text value : values) {
            String[] tokens = value.toString().split(" ");
            goals += Integer.parseInt(tokens[1]);
            assists += Integer.parseInt(tokens[2]);
            seasonCount++;
        }
        str = String.valueOf(seasonCount) + "个赛季平均进球" + goals / seasonCount
                + ",平均助攻" + assists / seasonCount;
        System.out.println("key:" + key + " value:" + str);
        context.write(key, new Text(str));
    }
}
