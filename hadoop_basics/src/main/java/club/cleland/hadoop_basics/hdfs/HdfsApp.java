package club.cleland.hadoop_basics.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;


public class HdfsApp {

    /**
     * 获取文件句柄
     */
    private FileSystem getFileSystem() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }


    /**
     * 读取文件
     * @param filePath
     */
    public void readHdfs(String filePath){
        FSDataInputStream fsDataInputStream = null;
        try{
            fsDataInputStream = this.getFileSystem().open(new Path(filePath));
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(fsDataInputStream != null){
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }


    /**
     * 写入文件
     * @param localFilePath
     * @param hdfsFilePath
     */
    public void writeHdfs(String localFilePath, String hdfsFilePath){
        FSDataOutputStream outputStream = null;
        FileInputStream inputStream = null;
        try{
            inputStream = new FileInputStream(localFilePath);
            outputStream = this.getFileSystem().create(new Path(hdfsFilePath));
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);


        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (inputStream != null){
                IOUtils.closeStream(inputStream);
            }
            if(outputStream != null){
                IOUtils.closeStream(outputStream);
            }
        }
    }


    /**
     * 主函数入口
     * @param args
     */
    public static void main(String[] args){
        HdfsApp hdfsapp = new HdfsApp();
        hdfsapp.readHdfs("/data/hdfs-site.xml");
        hdfsapp.writeHdfs("/Users/cleland/Desktop/core-site.xml", "/data/core-site.xml");
    }

}
