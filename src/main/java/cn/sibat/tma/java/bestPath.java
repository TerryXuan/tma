package bianli;

import pre_process.HttpRequst;
import pre_process.data_process;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.json.JSONException;


public class bestPath {
	public static File file1 = new File("source/aircraft.csv");
	public static File file2 = new File("source/airport.txt");
	public static File file3 = new File("source/dura.csv");
	public static File file4 = new File("source/order.csv");

	
	static SimpleDateFormat formatter=new SimpleDateFormat("H:mm:ss");
	

	static HashMap<String,String[]> aircraft =new HashMap<String, String[]>();
	static HashMap<String,String[]> aircraftcopy1 =new HashMap<String, String[]>();
	static HashMap<String,String[]> airport =new HashMap<String, String[]>();
	static HashMap<String,String[]> dura =new HashMap<String, String[]>();
	static HashMap<String,String[]> order =new HashMap<String, String[]>();
	static HashMap<String,String[]> ordercopy1 =new HashMap<String, String[]>();
	static HashMap<String,String[]> departing =new HashMap<String, String[]>();
	static HashMap<String,String[]> departingcopy1 =new HashMap<String, String[]>();
	static HashMap<String,List<String>> output =new HashMap<String, List<String>>();

	

	//读取飞机信息
	public static Integer readaircraft(){
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scanner scan = new Scanner(fis,"gbk");
		scan.nextLine();
		while(scan.hasNext()){
			String line = scan.nextLine();
			String[] lines = line.split(",");
			String key =lines[0];
			String time1 ="5:55:00";
			String time2 ="18:00:00";
			//key为飞机代码
			String[] value = {lines[1],lines[2],lines[4],lines[6],time1,time2,"0"};
			//value为飞机位置，飞机可飞行时长，飞机剩余座位，飞机可受重量，飞机时间1，飞机时间2
			aircraft.put(key, value);
		}
		return null;
	}
	//读取酒店机场信息
	public static Integer readairport(){
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file2);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scanner scan = new Scanner(fis,"gbk");
		scan.nextLine();
		while(scan.hasNext()){
			String line = scan.nextLine();
			String[] lines = line.split(",");
			String key =lines[0];
			//key为酒店机场代码
			String[] value = {lines[1],lines[2]};
			//value为是否能加油，最大飞机停留数
			airport.put(key, value);
		}
		return null;
	}
	//读取酒店机场间距离与时间
	public static Integer readdura(){
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file3);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scanner scan = new Scanner(fis,"gbk");
		scan.nextLine();
		while(scan.hasNext()){
			String line = scan.nextLine();
			String[] lines = line.split(",");
			String key =lines[0]+"-"+lines[1];
			//key为起点-终点
			String[] value = {lines[2],lines[3]};
			//value为是距离/km，时间/min
			dura.put(key, value);
		}
		return null;
	}
	//读取订单信息
	public static Integer readorder(){
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file4);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scanner scan = new Scanner(fis,"gbk");
		scan.nextLine();
		while(scan.hasNext()){
			String line = scan.nextLine();
			String[] lines = line.split(",");
			String key =lines[0];
			//key为订单号
			String[] value = {lines[2],lines[3],lines[4],lines[5],lines[6],lines[7],lines[8],lines[9]};
			//value为是往返方向，机场或酒店名称，最大间隔，乘客数，乘客重量，行李重量，最早时间，最晚时间
			order.put(key, value);
		}
		return null;
	}
	//安排时间窗早的departing的订单
	public static Integer departing() throws ParseException{
		ordercopy1=  (HashMap<String, String[]>) order.clone();
		String minendtime = null;
		//选出departing订单中endtime最早的
		for (Iterator<Map.Entry<String, String[]>> it = ordercopy1.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, String[]> item = it.next();
	        String[] value = item.getValue();
	        String endtime = "19:00:00";
	        if(value[0].equals("Departing")
	        		&& formatter.parse(endtime).getTime()-formatter.parse(value[7]).getTime()>0) {
	         endtime = value[7];
	         minendtime =endtime;
	        }
	        it.remove();
		}
		ordercopy1=  (HashMap<String, String[]>) order.clone();
		//获取departing订单中minendtime大于starttime的所有订单
		for (Iterator<Map.Entry<String, String[]>> it = ordercopy1.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, String[]> item = it.next();
	        String[] value = item.getValue();
	        String key = item.getKey();
	        if(value[0].equals("Departing")
	        		&& formatter.parse(minendtime).getTime()-formatter.parse(value[6]).getTime()>0) {
	         departing.put(key, value);
	        }
	        it.remove();
		}
		
		//先安排头批departing订单
		departingcopy1=  (HashMap<String, String[]>) departing.clone();
		aircraftcopy1=  (HashMap<String, String[]>) aircraft.clone();
		
		for (Iterator<Map.Entry<String, String[]>> it = aircraftcopy1.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, String[]> item = it.next();
	        String[] value1 = item.getValue();
	        String key1 = item.getKey();
	        String name1 =value1[0];
			for (Iterator<Map.Entry<String, String[]>> it1 = departingcopy1.entrySet().iterator(); it1.hasNext();){
				Map.Entry<String, String[]> item1 = it1.next();
		        String[] value2 = item1.getValue();
		        String key2 = item1.getKey();
		        String name2 =value2[1];
		        if(name2.equals(name1)) {
		        	int seat= Integer.parseInt(value1[2])-Integer.parseInt(value2[3]);
		        	int weight = Integer.parseInt(value1[3])-Integer.parseInt(value2[4])-Integer.parseInt(value2[5]);
		        	String time1 = value2[6];
		        	String time2 = value2[7];
		        	String[] value = {name2,value1[1],String.valueOf(seat),String.valueOf(weight),time1,time2,"1"};
		        	aircraft.put(key1, value);
		        	List<String> list = new LinkedList<String>();
		        	list.add(key2);
		        	output.put(key1, list);
		        	departing.remove(key1);
		        	order.remove(key1);
		        }        
		        it1.remove();
			}        
	        it.remove();
		}
		departingcopy1=  (HashMap<String, String[]>) departing.clone();
		aircraftcopy1=  (HashMap<String, String[]>) aircraft.clone();
		for (Iterator<Map.Entry<String, String[]>> it = aircraftcopy1.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, String[]> item = it.next();
	        String[] value1 = item.getValue();
	        String key1 = item.getKey();
	        String name1 =value1[0];
	        String type =value1[6];
	        if(!type.equals(0)) {
			for (Iterator<Map.Entry<String, String[]>> it1 = departingcopy1.entrySet().iterator(); it1.hasNext();){
				Map.Entry<String, String[]> item1 = it1.next();
		        String[] value2 = item1.getValue();
		        String key2 = item1.getKey();
		        String name2 =value2[1];
		        if(name2.equals(name1)) {
		        	int seat= Integer.parseInt(value1[2])-Integer.parseInt(value2[3]);
		        	int weight = Integer.parseInt(value1[3])-Integer.parseInt(value2[4])-Integer.parseInt(value2[5]);
		        	String time1 = value2[6];
		        	String time2 = value2[7];
		        	String[] value = {name2,value1[1],String.valueOf(seat),String.valueOf(weight),time1,time2,"1"};
		        	aircraft.put(key1, value);
		        	List<String> list = new LinkedList<String>();
		        	list.add(key2);
		        	output.put(key1, list);
		        	departing.remove(key1);
		        	order.remove(key1);
		        }        
		        it1.remove();
			}
	        }
	        it.remove();
		} 
		return null;
	}
	public static void main(String[] args) {

	}
}