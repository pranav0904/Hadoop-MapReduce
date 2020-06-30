import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<Text, CompositeValueWritable> {

    @Override
    public int getPartition(Text key, CompositeValueWritable value, int noOfPartitions) {
        return key.hashCode() % noOfPartitions;
    }
}