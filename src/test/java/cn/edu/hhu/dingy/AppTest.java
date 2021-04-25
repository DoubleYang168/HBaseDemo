package cn.edu.hhu.dingy;

import static org.junit.Assert.assertTrue;

import cn.edu.hhu.dingy.hbase.HbaseDML;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void readHbaseByRowKeyTest() throws IOException {
        HbaseDML hbaseDML = new HbaseDML();
        String rowKey = "1001";
        hbaseDML.getRow("ET_1Day_1km_hanjiang_20100101", rowKey);
    }

    @Test
    public void testForLoop(){
        for (int i = 0; i < 984; i++) {
            for (int j = 0; j < 465; j++) {
                System.out.println(j);
            }
            System.out.println("-----------------"+i+"--------------------");
        }
    }
}
