import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Arrays;


public class HdfsTest {
    public static final String HDFS_PATH = "hdfs://192.168.80.128:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
        System.out.println("HDFS Application SETUP");
    }

    /**
     * 上传一个文件，并写入hello hadoop
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        //参数一：路径  参数二：是否重写
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"), false);
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 查看一个文件的内容
     * @throws IOException
     */
    @Test
    public void cat() throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    /**
     * 新建一个文件夹
     * @throws IOException
     */
    @Test
    public void mkDir() throws IOException {
        //fileSystem.mkdirs(new Path("/hdfsapi/test"));
        //创建文件夹时，指定权限信息，但创建的结果不会跟指定的信息完全一致
        //因为指定的参数值还要经过一个参数值的掩码运算
        //fs.permissions.umask-mode，掩码值是002，因为会覆盖掉组合其他人的写权限
        //解决方法：修改配置文件，将参数值改为000
        fileSystem.mkdirs(new Path("/hdfsapi/test"), new FsPermission((short) 722));
    }

    /**
     * 下载一个文件
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        //获取输入流
        InputStream inputStream = fileSystem.open(new Path("/lewy/firstshell.sh"));
        //获取输出流
        OutputStream outputStream = new FileOutputStream(new File("D:/firstshell.sh"));
        IOUtils.copyBytes(inputStream,outputStream,configuration);
        inputStream.close();
        outputStream.close();
    }

    /**
     * 读取文件中指定偏移量范围的数据(从头开始)
     */
    @Test
    public void testReadFilePart1() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("qingshu.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("d:/qingshu-part.txt");
        byte[] b = new byte[10];
        int len = 0;
        long count = 0;
        while ((len = fsDataInputStream.read(b))!= -1) {
            fileOutputStream.write(b);
            count += len;
            if (count == 20 ) break;
        }
        fileOutputStream.flush();
        fileOutputStream.close();
        fsDataInputStream.close();
    }

    /**
     * 读取文件中指定偏移量范围的数据(从中间开始，即有偏移量)
     */
    @Test
    public void testReadFilePart2() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("qingshu.txt"));
        //seek()方法：将读取的位置设置为指定的起始位置
        fsDataInputStream.seek(20);
        FileOutputStream fileOutputStream = new FileOutputStream("d:/qingshu-part.txt");
        byte[] b = new byte[10];
        int len = 0;
        long count = 0;
        while ((len = fsDataInputStream.read(b))!= -1) {
            fileOutputStream.write(b);
            count += len;
            if (count == 20 ) break;
        }
        fileOutputStream.flush();
        fileOutputStream.close();
        fsDataInputStream.close();
    }

    /**
     * 重命名
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        //重命名：oldPath newPath
        fileSystem.rename(new Path("/student"), new Path("/newStu"));
    }

    /**
     * 删除目录(remove 或 delete)
     * @throws IOException
     */
    @Test
    public void remove() throws IOException {
        //delete  参数一：指定删除的路径，参数二：布尔类型，是否递归删除
        //如果是true，则递归删除，如果为false，则只能删除空目录
        //fs.delete(new Path("/park3"),true);
        fileSystem.delete(new Path("/techer"), true);
    }

    /**
     * 查看指定目录下有哪些文件或目录,相当于ls
     * @throws IOException
     */
    @Test
    public void listDir() throws IOException {
        //listFiles 参数一：指定查看目录  参数二：是否递归，boolean，true表示递归
        //注意：返回的是迭代器的好处：
        //返回集合是存放在内存中，若集合太大，会占用太多内存
        //但是返回迭代器，是一个对象，next后才会拿一个
        RemoteIterator<LocatedFileStatus> it=
                fileSystem.listFiles(new Path("/MyStudent"), true);
        while(it.hasNext()){
            LocatedFileStatus file = it.next();
            System.out.println("文件的分块大小:" + file.getBlockSize());
            System.out.println("最近访问时间:" + file.getAccessTime());
            System.out.println("文件的所属组:" + file.getGroup());
            System.out.println("文件的总长度:" + file.getLen());
            System.out.println("文件的副本数:" + file.getReplication());
            System.out.println("最近修改时间:" + file.getModificationTime());
            System.out.println("文件的所属者:" + file.getOwner());
            System.out.println("文件的全路径:" + file.getPath());
            System.out.println("文件的权限信息:" + file.getPermission());
            System.out.println("文件的块的位置信息:" + Arrays.toString(file.getBlockLocations()));
            System.out.println("-----------------------");
        }
    }

    @Test
    public void listDir2() throws IOException {
        //listStatus返回的是一个数组，没有递归，只遍历一级子节点
        FileStatus[] files =
                fileSystem.listStatus(new Path("/MyStudent"));

        for (FileStatus file: files) {
            System.out.println(file.getPath());
            System.out.println(file.isDirectory()? "d" : "r");
            System.out.println("-------------------------");
        }
    }

    @Test
    public void getFileBlockLocations() throws Exception{
        //--①参：指定路径  ②参：start 查看块的起始范围  ③参：查看块的结束范围
        //--我们可以通过 start 和 length的范围来调整具体查看哪一块。
        //--大多数的场景是查看所有块信息。 所以参数为：start=0 ; length=Integer.Maxvalue
        BlockLocation[] blks=
                fileSystem.getFileBlockLocations(new Path("/jdk/jdk-8u181-linux-x64.tar.gz"),0,Integer.MAX_VALUE);
        for(BlockLocation blk:blks){
            System.out.println(blk);
        }
        //--针对打印结果的说明，
        //--例子：
        //  0,134217728,hadoop01
        //  134217728,51429104,hadoop01
        //--0表示第一块的起始位置  ;134217728表示块的实际大小即128M; hadoop01表示存储的datanode服务器
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
        configuration = null;
        System.out.println("HDFS Application SHUTDOWN");
    }
}
