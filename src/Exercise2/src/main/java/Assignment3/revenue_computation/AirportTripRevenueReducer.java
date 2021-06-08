package Assignment3.revenue_computation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportTripRevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    
        double sum = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        
        DoubleWritable tripValue = new DoubleWritable(sum);
        context.write(key, tripValue);
    }
}
