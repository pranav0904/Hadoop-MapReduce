import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<Text, DoubleWritable> {
    @Override
    public int getPartition(Text key, DoubleWritable doubleWritable, int noOfPartitions) {
        return key.hashCode() % noOfPartitions;
    }
}
