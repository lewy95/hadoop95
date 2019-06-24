package cn.xzxy.yjt.sortGoals;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Goal implements Writable {
    private String name;
    private int goals;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getGoals() {
        return goals;
    }

    public void setGoals(int goals) {
        this.goals = goals;
    }

    @Override
    public String toString() {
        return "Goals{" +
                "name='" + name + '\'' +
                ", goals=" + goals +
                '}';
    }

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(goals);
    }

    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.goals = dataInput.readInt();
    }
}
