/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
///hadoop jar C:\Users\Manuel\Documents\NetBeansProjects\BigDataTest\dist\BigDataTest.jar C:\Users\Manuel\Documents\BigDataTest\input C:\Users\Manuel\Documents\BigDataTest\output
package bigdatatest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author Manuel
 */
public class BigDataTest {

    /**
     * @param args the command line arguments
     */
    static class MapReadSort extends Mapper<LongWritable, Text, KeyValue, DoubleWritable> {

        private static DoubleWritable time = new DoubleWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //SPLITING THE VALUES BY TAB
            String[] values = value.toString().split("\t");
            //SELECTING THE THE SOLVED INSTANCES AND SKIPPING THE FIRST ROW OF THE TABLE
            if (!values[0].equals("Solver") && values[14].equals("solved")) {
                double timeAux = Double.parseDouble(values[11]);
                time.set(timeAux);
                context.write(new KeyValue(new Text(values[0]), time), time);
            }
        }
    }

    static class ReducerRow extends Reducer<KeyValue, DoubleWritable, Text, Text> {

        @Override
        protected void reduce(KeyValue key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String row = "";
//            HERE WE GOT SORTED VALUES BY TIME
            for (DoubleWritable value : values) {
                row += (" " + value.get());
            }
            context.write(key.key, new Text(row));
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cactus Plot");
        String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        job.setMapperClass(MapReadSort.class);
        job.setReducerClass(ReducerRow.class);

        job.setOutputKeyClass(KeyValue.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

//      SORTING THE DATA IN SORT STAGE USING A COMPARATOR 
        job.setGroupingComparatorClass(KeyComparator.class);

        FileInputFormat.setInputPaths(job, new Path(myArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(myArgs[1] + "/first"));

        job.waitForCompletion(true);

        /////////////////////////////////////////////
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Cactus Plot 2");

        job2.setMapperClass(MapInverse.class);
        job2.setReducerClass(ReducerInverse.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(myArgs[1] + "/first/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(myArgs[1] + "/final"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

        /////////////////////////////////////////////       
    }

    static class MapInverse extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] sValues = value.toString().split(" ");
            long rowN = 0;
            for (String sValue : sValues) {
                context.write(new LongWritable(rowN), new Text(key.get() + "~" + sValue));
                rowN++;
            }

        }
    }

    static class ReducerInverse extends Reducer<LongWritable, Text, Text, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            TreeMap<Long, String> row = new TreeMap<Long, String>();

            for (Text value : values) {
                String rowValue = value.toString();
                String[] rowParts = rowValue.split("~");

                row.put(Long.valueOf(rowParts[0]), rowParts[1]);
            }
            String rowString = StringUtils.join("\t", row.values());
            context.write(new Text(rowString), new Text());
        }
    }

    static class KeyComparator extends WritableComparator {

        public KeyComparator() {
            super(KeyValue.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            KeyValue key1 = (KeyValue) o1;
            KeyValue key2 = (KeyValue) o2;
            return key1.key.compareTo(key2.key);
        }

    }

    static class KeyValue implements Writable, WritableComparable<KeyValue> {

        private Text key;
        private DoubleWritable value;

        public KeyValue() {
            this.key = new Text();
            this.value = new DoubleWritable();
        }

        public KeyValue(Text key, DoubleWritable value) {
            this.key = key;
            this.value = value;
        }

        public void set(Text key, DoubleWritable value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            key.readFields(in);
            value.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            key.write(out);
            value.write(out);
        }

        @Override
        public int compareTo(KeyValue o) {
            int compareValue = this.key.compareTo(o.key);
            if (compareValue == 0) {
                compareValue = value.compareTo(o.value);
            }
            return compareValue;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof KeyValue) {
                KeyValue aux = (KeyValue) o;
                return key.equals(aux.key) && value.equals(aux.value);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }
}
