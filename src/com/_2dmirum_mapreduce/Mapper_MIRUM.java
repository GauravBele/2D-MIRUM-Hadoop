package com._2dmirum_mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_MIRUM extends
		Mapper<LongWritable, Text, DoubleWritable, Text> {
	String queryImageVector;

	public Mapper_MIRUM(){
		File file = new File("./src/resource/localQueryImage.txt");
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(file));
			queryImageVector =br.readLine();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//queryImageVector="42327 1019 826 1106 4688 4715 3174 1980 1399 1035 880 703 549 653 394 88";
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String input = value.toString();
		String chunck[] = input.split("\t");
		double distance = getEuclideanDistance(chunck[0]);
		context.write(new DoubleWritable(distance), new Text(chunck[1] + " "
				+ chunck[2] + " " + chunck[3]));
	}

	public double getEuclideanDistance(String vector) {
		String vectorChuncks[] = vector.split(" ");
		String vectorChuncksQueryImage[]=queryImageVector.split(" ");
		double distence = 0,t1,t2;
		for (int i = 0; i < 16; i++) {
			t1=Double.parseDouble(vectorChuncks[i]);
			t2=Double.parseDouble(vectorChuncksQueryImage[i]);
			distence+=(t1-t2)*(t1-t2);
		}
		distence=Math.sqrt(distence);
		return distence;
	}
}
