package pds_pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Temperature {

    public static class AvgTuple implements Writable {
        private int Max = 0; private int Counter_min = 0;
        private int Min = 0; private int Counter_max = 0;

        @Override
        public void readFields(DataInput in) throws IOException {
            Min = in.readInt();  Counter_min = in.readInt();
            Max = in.readInt();  Counter_max = in.readInt();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(Min);
            out.writeInt(Counter_min);
            out.writeInt(Max);
            out.writeInt(Counter_max);
        }

        public int getMin() {
            return Min;
        }

        public void setMin(int Min) {
            this.Min = Min;
        }

        public int getCounter_min() {
            return Counter_min;
        }

        public void setCounter_min(int Counter_min) {
            this.Counter_min = Counter_min;
        }

        public int getMax() {
            return Max;
        }

        public void setMax(int Max) {
            this.Max = Max;
        }

        public int getCounter_max() {
            return Counter_max;
        }

        public void setCounter_max(int Counter_max) {
            this.Counter_max = Counter_max;
        }

        public String toString() {
            return "Minimal Temperature " + (1.0 * Min/Counter_min) + ", Max Temperature: " + (1.0 * Max/Counter_max);
        }

    }


    public static class TempMapper extends Mapper<Object, Text, Text, AvgTuple> {
        private Text Month = new Text();
        private AvgTuple outTuple = new AvgTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            Month.set(line[1].substring(4,6));
            int temperature = Integer.parseInt(line[3]);

            //TMAX or TMIN
            String extreme = line[2];
            if(extreme.equals("TMIN")){
                outTuple.setMin(temperature);
                outTuple.setCounter_min(1);
            }else if(extreme.equals("TMAX")){
                outTuple.setMax(temperature);
                outTuple.setCounter_max(1);
            }

            context.write(month, outTuple);
        }
    }

    public static class TempReducer extends Reducer<Text, AvgTuple, Text, AvgTuple> {

        private AvgTuple resultTuple = new AvgTuple();

        public void reduce(Text key, Iterable<AvgTuple> tuples, Context context) throws IOException, InterruptedException {
            int S_min = 0;  int S_max = 0;
            int C_min = 0;  int C_max = 0;

            for(AvgTuple tup : tuples){
                S_min += tup.getMin();   S_max += tup.getMax();
                C_min += tup.getCounter_min();  C_max += tup.getCounter_max();
            }

            resultTuple.setMin(S_min);
            resultTuple.setCounter_min(C_min);
            resultTuple.setMax(S_max);
            resultTuple.setCounter_max(C_max);

            context.write(key, resultTuple);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average extreme temperature");
        job.setJarByClass(AvgTemp.class);
        job.setMapperClass(TempMapper.class);
        job.setCombinerClass(TempReducer.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}