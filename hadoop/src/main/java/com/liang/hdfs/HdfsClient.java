/**   
* @Title: HDFSDemo.java  
* @Package com.liang.hdfs  
* @Description: TODO(用一句话描述该文件做什么)  
* @author mmj     
* @date 2018年11月25日 上午7:49:02  
* @version V1.0    
*/  

package com.liang.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/** 
* java 访问 Hdfs 客户端 例子代码 
*    
*/
public class HdfsClient {
	private FileSystem  fs =null;

	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{

		// 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
		// 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
		// new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
		// 然后再加载classpath下的hdfs-site.xml
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://min1:9000");
		/**
		 * 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
		 */
		conf.set("dfs.replication", "3");

		// 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
		// fs = FileSystem.get(conf);

		// 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
		fs = FileSystem.get(new URI("hdfs://min1:9000"), conf, "hadoop");
	}
	
	/**
	 * @TODO 上传文件到HDFS
	 */
	@Test
	public void testAddFileToHdfs() throws Exception{
		// 要上传的文件所在的本地路径
		Path src= new Path("F:\\staday-video.avi");
		// 要上传到hdfs的目标路径
		Path dst= new Path("/test_dir/");
		if(!fs.exists(dst))fs.mkdirs(dst);
		try {
			fs.copyFromLocalFile(src, dst);
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	 * @TODO 从HDFS中下载文件到本地
	 */
	@Test
	public void testDownloadFileToLocal() throws Exception{
		fs.copyToLocalFile(
				false,//是否删除原文件
				new Path("/test_dir/staday-video.avi"),//源路径
				new Path("e:/"),//目标路径
				true //目标路径是否本地文件系统
				);
		fs.close();
	}
	
	@Test
	public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {

		
		// 创建目录
		fs.mkdirs(new Path("/test_new/a1/b1"));

		// 删除文件夹 ，如果是非空文件夹，参数2必须给值true
		fs.delete(new Path("/aaa"), true);

		// 重命名文件或文件夹
		fs.rename(new Path("/test_new"), new Path("/test_n"));

	}
	/**
	 * 查看目录信息，只显示文件
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getBlockSize());
			System.out.println(fileStatus.getPermission());
			System.out.println(fileStatus.getLen());
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation bl : blockLocations) {
				System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
				String[] hosts = bl.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("--------------打印的分割线--------------");
		}
	}

}
