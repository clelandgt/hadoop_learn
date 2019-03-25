package club.cleland.hadoop_basics.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;


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
            // IOUtils.copyBytes的方式拷贝数据输出
            fsDataInputStream = this.getFileSystem().open(new Path(filePath));
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);

            // seek()
            System.out.print("\ngo back to the start of the file\n");
            fsDataInputStream.seek(0);
            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);

            // TODO: 补充read, readFull函数的使用

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
     * 删除文件
     * @param filePath
     * @return
     * @throws Exception
     */
    public boolean deleteHdfs(String filePath) throws Exception{
            return this.getFileSystem().delete(new Path(filePath), true);
    }

    /**
     * 创建文件夹
     * @param dir
     * @return
     * @throws Exception
     */
    public boolean mkdirHdfs(String dir) throws Exception {
        return this.getFileSystem().mkdirs(new Path(dir));
    }


    /**
     * 主函数入口
     * @param args
     */
    public static void main(String[] args) throws Exception{
        HdfsApp hdfsapp = new HdfsApp();
        String destFile = "/Users/cleland/Desktop/core-site.xml";
        String targetFile = "/data/intput/core-site.xml";
        hdfsapp.mkdirHdfs("/data/input");
        hdfsapp.writeHdfs(destFile, targetFile);
        hdfsapp.readHdfs("/data/hdfs-site.xml");
        hdfsapp.deleteHdfs(targetFile);
        hdfsapp.writeHdfs(destFile, targetFile);

    }

}
