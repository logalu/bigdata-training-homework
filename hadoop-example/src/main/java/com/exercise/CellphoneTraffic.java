package com.exercise;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CellphoneTraffic implements Writable {
    private String phone;
    private long uploadTraffic;
    private long downloadTraffic;
    private long totalTraffic;

    public CellphoneTraffic() {}
    public CellphoneTraffic(String phone, long uploadTraffic, long downloadTraffic) {
        super();
        this.phone = phone;
        this.uploadTraffic = uploadTraffic;
        this.downloadTraffic = downloadTraffic;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.phone);
        dataOutput.writeLong(this.uploadTraffic);
        dataOutput.writeLong(this.downloadTraffic);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.uploadTraffic = dataInput.readLong();
        this.downloadTraffic = dataInput.readLong();
    }

    public long getUploadTraffic() {
        return uploadTraffic;
    }

    public void setUploadTraffic(long uploadTraffic) {
        this.uploadTraffic = uploadTraffic;
    }

    public long getDownloadTraffic() {
        return downloadTraffic;
    }

    public void setDownloadTraffic(long downloadTraffic) {
        this.downloadTraffic = downloadTraffic;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(long totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    @Override
    public String toString() {
        return this.uploadTraffic + " " + this.downloadTraffic + " " + this.totalTraffic;
    }
}
