package cn.xzxy.yjt.fruitTotal;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Fruit implements Writable {

    private String name;
    private String addr;
    private int weigh;
    private double price;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public int getWeigh() {
        return weigh;
    }

    public void setWeigh(int weigh) {
        this.weigh = weigh;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Fruit{" +
                "name='" + name + '\'' +
                ", addr='" + addr + '\'' +
                ", weigh=" + weigh +
                ", price=" + price +
                '}';
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(addr);
        dataOutput.writeInt(weigh);
        dataOutput.writeDouble(price);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.addr = dataInput.readUTF();
        this.weigh = dataInput.readInt();
        this.price = dataInput.readDouble();
    }
}
