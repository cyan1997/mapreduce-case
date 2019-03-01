package hadoop2;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
 
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
 * �������2�����ƶȾ��� �� (ת�����־���)���
 * input:
 	1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
	2	1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
	3	4_0.86,3_1.00,1_0.93
	4	1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
	5	2_0.29,5_1.00,6_0.71
	6	1_0.26,5_0.71,6_1.00,2_0.88,4_0.36
 * 
 * cache:
 * 	A	6_5,4_3,2_10,1_2
	B	6_5,5_3,2_3
	C	4_5,3_15,1_5
	
	output:
	1	A_9.87,B_2.38,C_23.90
	2	A_16.59,B_8.27,C_4.25
	3	C_23.95,A_4.44
	4	B_3.27,C_22.85,A_11.68
	5	A_6.45,B_7.42
	6	C_3.10,A_15.40,B_9.77
	
	�磺
	map
	1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
	��
	A	6_5,4_3,2_10,1_2
	=
	1.00*2+0.36*10+0.99*3+0.26*5
	=9.87
	���ɣ�1,A_9.9��
	reduce �����еĺϲ������Ƽ��б�
 * @author chenjie
 *
 */
public class Step4 {
	public static class Mapper4 extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		private List<String> cacheList = new ArrayList<String>();
		
		private DecimalFormat df = new DecimalFormat("0.00");
		
		/***
		 * 	�������Ҳ������ļ����浽�ڴ��У�ÿһ��Ϊһ���ַ������������й���list
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			FileReader fr = new FileReader("myfile");
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null)
			{
				cacheList.add(line);
				System.out.println("----------------------cache line :" + line);
			}
			fr.close();
			br.close();
		}
		
		
		/*	�������߼���ʽ
		 * 1	2	-2	0
		 * 3	3	4	-3
		 * -2	0	2	3
		 * 5	3	-1	2
		 * -4	2	0	2
		 * ������������ʽ
		 * 1	1_1,2_2,3_-2,4_0
		 * 2	1_3,2_3,3_4,4_-3
		 * 3	1_-2,2_0,3_2,4_3
		 * 4	1_5,2_3,3_-1,4_2
		 * 5	1_-4,2_2,3_0,4_2
		 * 
		 * �Ҳ������ת�ã�������ʽ
		 *  1	3_0,1_0,4_-2,2_1
			2	3_1,4_2,2_3,1_3
			3	4_-1,1_-1,3_4,2_5
			4	1_2,3_-1,4_1,2_-2
			5	4_2,3_2,1_-3,2_-1
			
			key: "1"
			value: "1	1_1,2_2,3_-2,4_0"
		 * */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("-------------------map,key=" + key + "value=" + value);
			String[] rowAndline = value.toString().split("\t");
			//����к�
			//rowAndline : {"1","1_1,2_2,3_-2,4_0"}
			String row_matrix1 = rowAndline[0];
			//row_matrix1 ��"1"
			String[] column_value_array_matrix1 = rowAndline[1].split(",");
			//��ø���
			//rowAndline[1] �� "1_1,2_2,3_-2,4_0"
			//column_value_array_matrix1 : {"1_1","2_2","3_-2","4_0"}
			for(String line : cacheList)// ��line:"3		4_-1,1_-1,3_4,2_5"Ϊ��
			{
				String[] rowAndline2 = line.toString().split("\t");
				//rowAndline2 : {"3","4_-1,1_-1,3_4,2_5"}
				String row_matrix2 = rowAndline2[0];
				//���ת�þ���line�е��кţ�ԭ�Ҿ�����кţ�
				String[] column_value_array_matrix2 = rowAndline2[1].split(",");
				//rowAndline2[1] : "4_-1,1_-1,3_4,2_5"
				//column_value_array_matrix2 : {"4_-1","1,-1","3_4","2_5"}
				double result = 0;
				//����ɼ��ۼӽ��
				for(String column_value_matrix1 : column_value_array_matrix1)//����������line�е�ÿһ��(����) "1_1","2_2","3_-2","4_0"
				{
					String column_maxtrix1 = column_value_matrix1.split("_")[0];
					//����к�
					String value_matrix1 = column_value_matrix1.split("_")[1];
					//��ø��е�ֵ
					
					for(String column_value_matrix2 : column_value_array_matrix2)//�����Ҳ�����line�е�ÿһ��(����) "4_-1","1,-1","3_4","2_5"
					{
						String column_maxtrix2 = column_value_matrix2.split("_")[0];
						//����к�
						String value_matrix2 = column_value_matrix2.split("_")[1];
						//��ø��е�ֵ
						
						if(column_maxtrix2.equals(column_maxtrix1))//����Ҳ������ΪʲôҪ�����кţ�ֻ���к���ȷ����ȣ���֤����ͬһ��λ�õķ���
						{
							result += Double.valueOf(value_matrix1) * Double.valueOf(value_matrix2);
							//result += 1 * (-1)
							//result += 2 * 5
							//result += -2 * 4
							//result += 0 * (-1)
						}
					}
				}
				if(result == 0)
					continue;
				
				outKey.set(row_matrix1);//�����keyֵ����Ϊ��������к�
				outValue.set(row_matrix2 + "_" +df.format(result));//�����valueֵ����Ϊ�Ҳ�ת�þ�����к�(ʵ�ʾ�����к�)_��λ�õ�ֵ
				context.write(outKey, outValue);
				//("1","3_1") 
			}
			//("1","2_7")("1,"3_1")("1","2_4")("1","4_0")("1","5_9")
			//("2","1_9")...
			//....
		}
	}
	
	
	public static class Reducer4 extends Reducer<Text,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		/**
		 * ��map������key-value�Խ�����ϣ�ƴ�ӳɽ�������������ʽ
		 * ("1","2_7")("1,"3_1")("1","2_4")("1","4_0")("1","5_9")
		 * ("2","1_9")...
		 * ...
		 * ����keyֵ��ͬ��Ԫ��("1","2_7")("1,"3_1")("1","2_4")("1","4_0")("1","5_9")
		 * �Ὣ�����
		 * key : "1"
		 * values : {"2_7","3_1","2_4","4_0","5_9"}
		 *
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			for(Text text : values)
			{
				sb.append(text + ",");
			}
			// sb : "2_7,3_1,2_4,4_0,5_9,"
			String line = "";
			if(sb.toString().endsWith(","))
			{
				line = sb.substring(0,sb.length()-1);
			}
			//line :"2_7,3_1,2_4,4_0,5_9"
			outKey.set(key);
			outValue.set(line);
			context.write(outKey, outValue);
			// ("1","2_7,3_1,2_4,4_0,5_9")
		}
		
	}
	
	
	private static final String INPATH = "hdfs://192.168.58.129:9000/output/tuijian2/part-r-00000";
	private static final String OUTPATH = "hdfs://192.168.58.129:9000/output/tuijian4";
	
	private static final String CACHE = "hdfs://192.168.58.129:9000/output/tuijian3/part-r-00000";
	private static final String HDFS = "hdfs://192.168.58.129:9000";
	
	public int run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
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
		    Job job = new Job(conf, "step4");//Job(Configuration conf, String jobName) ����job���ƺ�
		    job.setJarByClass(Step4.class);
		    job.setMapperClass(Mapper4.class); //Ϊjob����Mapper�� 
		    //job.setCombinerClass(IntSumReducer.class); //Ϊjob����Combiner��  
		    job.setReducerClass(Reducer4.class); //Ϊjob����Reduce�� 
 
		    job.addCacheArchive(new URI(CACHE + "#myfile"));
		    
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
			new Step4().run();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

