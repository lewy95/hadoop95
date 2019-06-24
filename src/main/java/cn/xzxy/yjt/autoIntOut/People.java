package cn.xzxy.yjt.autoIntOut;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class People implements Writable {

    private String name;
    private int age;
    private int weight;
    private int cost;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeInt(weight);
        dataOutput.writeInt(cost);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.weight = dataInput.readInt();
        this.cost = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "People{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", weight=" + weight +
                ", cost=" + cost +
                '}';
    }
}
