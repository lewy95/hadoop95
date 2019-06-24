package cn.xzxy.yjt.timeOfApp;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class App implements Writable {

    private String type;
    private String name;
    private double time;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getTime() {
        return time;
    }

    public void setTime(double time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "App{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", time=" + time +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(name);
        dataOutput.writeDouble(time);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.time = dataInput.readDouble();
    }
}
