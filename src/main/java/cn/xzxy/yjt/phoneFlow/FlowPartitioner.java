package cn.xzxy.yjt.phoneFlow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, Flow>{

    /**
     * 分区：指定哪个reduce处理
     * @param text
     * @param flow
     * @param i 对应设置的分区，设置了3个分区，所以为0 1 2
     * @return
     */
    public int getPartition(Text text, Flow flow, int i) {
        if (flow.getAddr().equals("bj")){
            return 0;
        }
        if (flow.getAddr().equals("sh")){
            return 1;
        }
        if (flow.getAddr().equals("sz")){
            return 2;
        }
        return 0;
    }


}
