package hadoopex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.List;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Iterator;
import java.io.IOException;

public class FindFriend
{
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text output =new Text();
        public void map(LongWritable key,Text value,Context context) throws InterruptedException,IOException
        {
            String[] input = value.toString().split("\t");
            String userid = input[0];
            //check the length 
            if (input.length == 2) 
            {
                String[] all_friends = input[1].split(",");
                List<String> friends_list = Arrays.asList(all_friends);
                for (String friend : friends_list)
                {
                    int user_id = Integer.parseInt(userid);
                    int friend_id = Integer.parseInt(friend);
                    if (user_id < friend_id)
                    {
                        output.set(userid+","+friend);
                    }
                    else
                    {
                        output.set(friend+","+userid);
                    }
                    context.write(output,new Text(input[1]));
                }
            }
        }
    }

    public static class MyReduce extends Reducer<Text,Text,Text,Text>
    {
        public void reduce (Text key, Iterable<Text> values, Context context) throws InterruptedException,IOException
        {
            String mutualfriends = "";
            List<String> mutualfriendslist = new LinkedList<>();
            Text[] data = new Text[2];
            Iterator<Text> iterator = values.iterator();
            int index =0;
            while (iterator.hasNext())
            {
                data[index++] = new Text(iterator.next());
            }
            String[] friendListUser =data[0].toString().split(",");
            String[] friendListFriend = data[1].toString().split(",");
            for(String i : friendListUser)
            {
                for (String j : friendListFriend)
                {
                    if(i.equals(j))
                    {
                        mutualfriendslist.add(i);
                    }
                }
            }
            for (int i=0; i<mutualfriendslist.size();i++)
            {
                if(i != mutualfriendslist.size() -1)
                {
                    mutualfriends += mutualfriendslist.get(i) + ",";
                }
                else
                {
                    mutualfriends += mutualfriendslist.get(i); 
                }
            }
            context.write(key,new Text(mutualfriends));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config,args).getRemainingArgs(); //get inputs
        if (otherArgs.length !=2) {
            System.err.println("Usage: Q1 Mutual Friends <inputfile hdfs path> <output file hdfs path>");
            System.exit(2);
        }
        @SuppressWarnings("deprecation")
		Job job = new Job(config,"FindFriend map reduce");
        job.setJarByClass(FindFriend.class);//add the class
        job.setMapperClass(MyMap.class);//add mapper class
        job.setReducerClass(MyReduce.class);//add reducer class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
