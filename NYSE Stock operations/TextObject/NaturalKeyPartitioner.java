import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int noOfPartitions) {
        return key.hashCode() % noOfPartitions;
    }
}