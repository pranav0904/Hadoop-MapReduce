import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<CompositeKeyWritable, IntWritable, Text, IntWritable> {

    IntWritable result = new IntWritable();
    Text text = new Text();
    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }
        text.set(key.getWebIp() + " \t " + key.getDate());
        result.set(sum);
        context.write(text, result);
    }
}