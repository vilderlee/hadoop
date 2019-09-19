package com.vilderlee.hadoop.check;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类说明:
 *
 * <pre>
 * Modify Information:
 * Author        Date          Description
 * ============ ============= ============================
 * VilderLee    2019/9/19      Create this file
 * </pre>
 */
public class Data implements Writable {

    private String data;

    public Data() {
    }

    public Data(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeUTF(getData());
    }

    @Override public void readFields(DataInput in) throws IOException {
        this.data = in.readUTF();
    }

    @Override public String toString() {
        return "Data{" + "data='" + data + '\'' + '}';
    }
}
