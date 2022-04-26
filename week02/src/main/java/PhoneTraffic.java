

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneTraffic implements Writable {
    private long up;
    private long down;

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }


    public PhoneTraffic() {
    }

    public PhoneTraffic(long up, long down) {
        this.up = up;
        this.down = down;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
    }

    @Override
    public String toString() {
        long sum = up + down;
        return "PhoneTraffic{" +
                "up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }
}
