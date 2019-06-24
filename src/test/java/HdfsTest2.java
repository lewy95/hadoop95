import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


public class HdfsTest2 {
    public static final String HDFS_PATH = "hdfs://192.168.80.128:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
        System.out.println("Hdfs Application starts");
    }

    /**
     * 1、在hdfs创建目录teacher
     * 3、在hdfs创建目录student，并在student目录下创建新目录Tom、LiMing、Jerry
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/mr"));
        //fileSystem.mkdirs(new Path("/student/Tom"));
        //fileSystem.mkdirs(new Path("/student/LiMing"));
        //fileSystem.mkdirs(new Path("/student/Jerry"));
    }

    /**
     * 2、在teacher目录下上传文件score.txt
     *
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/teacher/score.txt"));
        //写入数据
        outputStream.write("hdfs".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 4、在Tom目录下上传information.txt,同时上传到LiMing、Jerry目录下
     *
     * @throws IOException
     */
    @Test
    public void create2() throws IOException {
        fileSystem.copyFromLocalFile(new Path("D:/fcb.txt"),new Path("/mr/fcb.txt"));
        //FSDataOutputStream outputStream1 = fileSystem.create(new Path("/student/Tom/information.txt"));
        //FSDataOutputStream outputStream2 = fileSystem.create(new Path("/student/LiMing/information.txt"));
        //FSDataOutputStream outputStream3 = fileSystem.create(new Path("/student/Jerry/information.txt"));
        //outputStream1.close();
        //outputStream2.close();
        //outputStream3.close();
    }

    /**
     * 5、将student重命名为MyStudent
     *
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        //重命名：oldPath newPath
        //fs.rename(new Path("/demo3"), new Path("/demo5"));
        fileSystem.rename(new Path("/student"), new Path("/MyStudent"));
    }

    /**
     * 6、将Tom下的information.txt下载到D:/tom目录中
     * 7、将teacher下的score.txt也下载到此目录
     *
     * @throws IOException
     */
    @Test
    public void get1() throws IOException {
        fileSystem.copyToLocalFile(new Path("/MyStudent/Tom/information.txt"),new Path("D:/tom/information.txt"));
        fileSystem.copyToLocalFile(new Path("/teacher/score.txt"),new Path("D:/tom/score.txt"));
    }

    /**
     * 8、删除hdfs中的Tom、LiMing目录
     *
     * @throws IOException
     */
    @Test
    public void remove() throws IOException {
        //删除目录remove 或 delete
        //delete  参数一：指定删除的路径，参数二：布尔类型，是否递归删除
        //如果是true，则递归删除，如果为false，则只能删除空目录
        //fs.delete(new Path("/park3"),true);
        fileSystem.delete(new Path("/MyStudent/Tom"), true);
        fileSystem.delete(new Path("/MyStudent/LiMing"), true);
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
        configuration = null;
        System.out.println("Hdfs Application stop");
    }
}
