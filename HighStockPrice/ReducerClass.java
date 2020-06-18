import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class ReducerClass extends Reducer<Text, FloatWritable,Text, FloatWritable> {
    FloatWritable result = new FloatWritable();
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float maxVal = 0;
        for (FloatWritable val: values) {
            if (maxVal< val.get()){
                maxVal = val.get();
            }
        }
        result.set(maxVal);
        context.write(key,result);
    }
}

/*

 */