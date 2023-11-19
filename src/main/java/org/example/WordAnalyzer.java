package org.example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAnalyzer {
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{

        // MapReduce invocará a este método una vez por cada línea del fichero
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Cogemos la línea que llega como parámetro, la convertimos a String
            // y la modificamos para quitar los caracteres que no son letras
            String valueString = value.toString();
            valueString = valueString.replaceAll("[^a-zA-Z]", "");

            // Convertimos todo a minúsculas para hacer el conteo sin distinción entre mayúsculas y minúsculas
            valueString = valueString.toLowerCase();


            String[] dataOfTheQuotation = valueString.split(" ");


            for(String w:dataOfTheQuotation){
                context.write(new Text(w), new IntWritable(1));
            }

        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();


        // pasando como parámetro todos los valores asociados generados en map.
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;

            // Simplemente sumamos los valores, y la suma será el resultado
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordAnalyzer");
        job.setJarByClass(WordAnalyzer.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}