package cn.xzxy.yjt.fcbPlayers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PlayerMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        System.out.println("------mapper");
        String[] tokens = value.toString().split("&");
        int season = Integer.parseInt(tokens[0].split(" ")[1]);
        int goal = Integer.parseInt(tokens[1].split(" ")[1]);
        int assist = Integer.parseInt(tokens[2].split(" ")[1]);

        String str = String.valueOf(season) + " " + String.valueOf(goal) + " "
                + String.valueOf(assist);

        System.out.println("key:" + key + " value:" + str);
        context.write(key, new Text(str));
    }
}
