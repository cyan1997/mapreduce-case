package hadoop2;
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
/**
 * �������:
 * 1��ת�����־���
 * 2�����ƶȾ��� �� (ת�����־���)
 * �������1��ת��
 * 
 * input:
 * 	1	A_2,C_5
	2	A_10,B_3
	3	C_15
	4	A_3,C_5
	5	B_3
	6	A_5,B_5
	output:
	A	6_5,4_3,2_10,1_2
	B	6_5,5_3,2_3
	C	4_5,3_15,1_5
 * @author chenjie
 *
 */
public class Step3 {
	public static class Mapper3 extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		
		//����ÿһ�У��Ե�һ��Ϊ��
		//key : 1
		//value : "1	1_0,2_3,3_-1,4_2,5_-3"
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] rowAndline = value.toString().split("\t");
			//rowAndline : {"1","1_0,2_3,3_-1,4_2,5_-3"}
			String row = rowAndline[0];
			//row "1"
			String[] lines = rowAndline[1].split(",");
			//rowAndline[1] : "1_0,2_3,3_-1,4_2,5_-3"
			//lines : {"1_0","2_3","3_-1","4_2","5_-3"}
			for(String line : lines)//����ÿһ�У��Ե�һ��Ϊ����line "1_0"
			{
				String colunm = line.split("_")[0];
				//colunm : 1
				String valueStr = line.split("_")[1];
				//valueStr : 0 
				outKey.set(colunm);
				//������Ϊ��
				outValue.set(row + "_" + valueStr);
				//������Ϊ��
				context.write(outKey, outValue);
				// ����(1,"1_0")
			}
			//ѭ������������{"1_0","2_3","3_-1","4_2","5_-3"}
			//����(1,"1_0") ��һ�У���һ��_0    (2,"1_3")  �ڶ��У���һ��_3		(3,"1_-1") (4,"1_2")(5,"1_-3")
			/*
			Ŀ��ת�þ���
			0	1	1	-2
			3	3	1	2
			-1	5	4	-1
			2	-2	-1	1
			-3	-1	2	2
			*/
			//���ö�Ӧ��ת�þ���ĵ�һ��
		}
		/*
			����map��������
			 ("1","1_0")	("2","1_3") 	("3","1_-1")	("4","1_2")		("5","1_-3")
			��"1","2_1"��	("2","2_3") 	("3","2_5")	    ("4","2_-2")	("5","2_-1")
			��"1","3_0"��	("2","3_1")	    ("3","3_4")		("4","3_-1")	("5","3_2")
			��"1","4_-2"��  ("2","4_2")	    ("3","4_-1")	("4","4_1")		("5","4_2")
		*/
 
	}
	
 
	/*
		Reduce���񣬽�map�������������м�ֵ�Լ��Ͻ��кϲ�������ת�þ���Ĵ洢��ʾ
		keyֵ��ͬ��ֵ�����ֵ�ļ���
		�磺
		key:"1"ʱ
		values:{"3_0","1_0","4_-2","2_1"} 
		ע�⣺�������ΪʲôҪ�����б�ŵ�ԭ��values��˳��һ������ԭ�������е�˳��
	*/
	
	public static class Reducer3 extends Reducer<Text,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			for(Text text : values)
			{
				sb.append(text + ",");
			}
			//sb : "3_0,1_0,4_-2,2_1,"
			//ע������ĩβ�и�����
			String line = "";
			if(sb.toString().endsWith(","))
			{
				line = sb.substring(0,sb.length()-1);
			}
			//ȥ������
			//line : "3_0,1_0,4_-2,2_1"
			outKey.set(key);
			outValue.set(line);
			//("1","3_0,1_0,4_-2,2_1")
			context.write(outKey, outValue);
		}
		
	}
	
	private static final String INPATH = "hdfs://192.168.58.129:9000/output/tuijian1/part-r-00000";//�����ļ�·��
	private static final String OUTPATH = "hdfs://192.168.58.129:9000/output/tuijian3";//����ļ�·��
	private static final String HDFS = "hdfs://192.168.58.129:9000";//HDFS·��
	
	public int run() throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		 conf.set("fs.defaultFS",HDFS);
		    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    //String[] otherArgs = {"hdfs://pc1:9000/input/chenjie.txt","hdfs://pc1:9000/output/out4"};
		    String[] otherArgs = {INPATH,OUTPATH};
		    //������Ҫ���ò���������������HDFS���ļ�·��
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		    }
		    //conf.set("fs.defaultFS",HDFS);
		   // JobConf conf1 = new JobConf(WordCount.class);
		    Job job = new Job(conf, "step3");//Job(Configuration conf, String jobName) ����job���ƺ�
		    job.setJarByClass(Step3.class);
		    job.setMapperClass(Mapper3.class); //Ϊjob����Mapper�� 
		    //job.setCombinerClass(IntSumReducer.class); //Ϊjob����Combiner��  
		    job.setReducerClass(Reducer3.class); //Ϊjob����Reduce�� 
 
		    job.setMapOutputKeyClass(Text.class);  
		    job.setMapOutputValueClass(Text.class); 
 
		    job.setOutputKeyClass(Text.class);        //�������key������
		    job.setOutputValueClass(Text.class);//  �������value������
 
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //Ϊmap-reduce��������InputFormatʵ����   ��������·��
 
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//Ϊmap-reduce��������OutputFormatʵ����  �������·��
		    FileSystem fs = FileSystem.get(conf);
			Path outPath = new Path(OUTPATH);
			if(fs.exists(outPath))
			{
				fs.delete(outPath, true);
			}
		    
			return job.waitForCompletion(true) ? 1 : -1;
		
		
		/*Configuration conf = new Configuration();
		conf.set("fs.defaultFS",HDFS);
		Job job = Job.getInstance(conf,"step1");
		job.setJarByClass(Step1.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(INPATH);
		if(fs.exists(inPath))
		{
			//FileInputFormat.addInputPath(conf, inPath);
		}
		Path outPath = new Path(OUTPATH);
		if(fs.exists(outPath))
		{
			fs.delete(outPath, true);
		}*/
		
	}
	
	public static void main(String[] args)
	{
		try {
			new Step3().run();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
