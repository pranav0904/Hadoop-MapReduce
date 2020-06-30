import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        double average = 0.0;
        double price =0.0;

        for (DoubleWritable val: values) {
            count+=1;
            price += val.get();
        }

        average =  price / count;

        result.set(average);
        context.write(key, result);
    }
}