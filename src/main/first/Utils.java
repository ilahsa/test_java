package first;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class Utils {
	public static void main(String[] args) throws Exception {
		String str = "a,b,c,,";
		String[] ary = str.split(",");
		System.out.println(ary.length);
	}
	/** 
     * Mapped File  way 
     * MappedByteBuffer 可以在处理大文件时，提升性能 
     * @param filename 
     * @return 
     * @throws IOException 
     */  
    public static byte[] readFileToByte(String filename)throws IOException{  
          
        FileChannel fc = null;  
        try{  
            fc = new RandomAccessFile(filename,"r").getChannel();  
            MappedByteBuffer byteBuffer = fc.map(MapMode.READ_ONLY, 0, fc.size()).load();  
            System.out.println(byteBuffer.isLoaded());  
            byte[] result = new byte[(int)fc.size()];  
            if (byteBuffer.remaining() > 0) {  
                byteBuffer.get(result, 0, byteBuffer.remaining());  
            }  
            return result;  
        }catch (IOException e) {  
            e.printStackTrace();  
            throw e;  
        }finally{  
            try{  
                fc.close();  
            }catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
    }
}
