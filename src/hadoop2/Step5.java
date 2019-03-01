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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
 
/***
 * ȥ���Ƽ��б��У��û��Ѿ�����������Ʒ�������û�A�Ѿ������iphone7����iphone7���Ƽ��б���ɾ��
 * input�����ƶȾ���
 *	1	A_9.87,B_2.38,C_23.90
	2	A_16.59,B_8.27,C_4.25
	3	C_23.95,A_4.44
	4	B_3.27,C_22.85,A_11.68
	5	A_6.45,B_7.42
	6	C_3.10,A_15.40,B_9.77
 * cache:������¼
 *  1	A_2,C_5
	2	A_10,B_3
	3	C_15
	4	A_3,C_5
	5	B_3
	6	A_5,B_5
	
	map:
	����
	1��Ʒ���Ƽ��б�1		A_9.87,B_2.38,C_23.90
	1��Ʒ�Ĳ�����¼��1		A_2,C_5
	�����1��Ʒ������A�Ѿ���2�֣�C�Ѿ���5��
	Ӧ�ð�A��C��1���Ƽ��б���ɾ����
	ֻ����B
	��������Ҫ�����û����Ƽ���Ʒ�����ǽ��û���Ϊkey,��Ʒ���Ƽ�����Ϊvalue����
	(B,1_2.38)
	
	reduce��
	��ͬһ�û��Ƽ�����Ʒ�ϲ����
	
	output:
	A	5_6.45,3_4.44
	B	4_3.27,1_2.38
	C	6_3.10,2_4.25
 * @author chenjie
 *
 */
public class Step5 {
 
	public static class Mapper5  extends Mapper<LongWritable,Text,Text,Text>
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
			FileReader fr = new FileReader("itemUserScore3");
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
		
		
		/**
		 * ��
		 * 	1��Ʒ���Ƽ��б�1		A_9.87,B_2.38,C_23.90
			1��Ʒ�Ĳ�����¼��1		A_2,C_5
			Ϊ��
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			System.out.println("-------------------map,key=" + key + "value=" + value);
			String item_matrix1 = value.toString().split("\t")[0];
			//�Ƽ��б���Ʒ�� 1
			String[] user_score_array_matrix1 = value.toString().split("\t")[1].split(",");
			//�Ƽ��б� A_9.87,B_2.38,C_23.90
			
			for(String line : cacheList)//��Ʒ�Ĳ�����¼�б�
			{
				String item_matrix2 = line.toString().split("\t")[0];
				//������¼��Ʒ�� 1
				
				String[] user_score_array_matrix2 = line.toString().split("\t")[1].split(",");
				//������¼  A_2,C_5
				
				if(item_matrix1.equals(item_matrix2))//����Ƽ��б���Ʒ��==������¼��Ʒ�ţ�֤����ͬһ��Ʒ�����ܲ���
				{
					for(String user_score : user_score_array_matrix1)//�����Ƽ��б���ÿһ���û�  A_9.87,B_2.38,C_23.90
					{
						boolean flag = false;//Ĭ�ϲ�������־λ
						String user_matrix1 = user_score.split("_")[0];
						//�û�ID 
						String score_matrix1 = user_score.split("_")[1];
						//�Ƽ���
						
						for(String user_score2 : user_score_array_matrix2)//���ڲ�����¼�е�ÿһ����¼  A_2,C_5
						{
							String user_matrix2 = user_score2.split("_")[0];
							//�û�ID
							if(user_matrix1.equals(user_matrix2))//�������ID��� ��A_9.87 ��A_2 ��֤���û�A����������Ʒ
							{
								flag = true;
							}
						}
						if(flag == false)//����û�Aû�в���������Ʒ
						{
							outKey.set(user_matrix1);//���û�ID��ΪKey
							outValue.set(item_matrix1 + "_" +score_matrix1 );//����ƷID_�Ƽ�����Ϊvalue
							context.write(outKey, outValue);//д������
						}
					}
				}
			}
		}
	}
	
	
	
	public static class Reducer5 extends Reducer<Text,Text,Text,Text>
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
	
	
	private static final String INPATH = "hdfs://192.168.58.129:9000/output/tuijian4/part-r-00000";
	private static final String OUTPATH = "hdfs://192.168.58.129:9000/output/tuijian5";
	
	private static final String CACHE = "hdfs://192.168.58.129:9000/output/tuijian1/part-r-00000";
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
		    job.setJarByClass(Step5.class);
		    job.setMapperClass(Mapper5.class); //Ϊjob����Mapper�� 
		    //job.setCombinerClass(IntSumReducer.class); //Ϊjob����Combiner��  
		    job.setReducerClass(Reducer5.class); //Ϊjob����Reduce�� 
 
		    job.addCacheArchive(new URI(CACHE + "#itemUserScore3"));
		    
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
			new Step5().run();
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 
}