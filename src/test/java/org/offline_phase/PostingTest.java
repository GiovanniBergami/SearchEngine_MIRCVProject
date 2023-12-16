package org.offline_phase;

import junit.framework.TestCase;
import org.common.PostingList;
import org.common.encoding.UnaryEncoder;
import org.common.encoding.VBEncoder;
import org.junit.Test;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PostingTest {

    @Test
    public void scrittura() throws IOException { //altro esempio a caso
        List<Integer> numeri= new ArrayList<>();
        numeri.add(1);
        numeri.add(2);
        numeri.add(3);

        String path = "data/prova.bin";
        ByteBuffer daScrivere = ByteBuffer.allocate(numeri.size()*4);
        System.out.println("noomeri "+numeri.size());

        for(Integer n :numeri){
            daScrivere.putInt(n);
        }
        daScrivere.flip();

        FileOutputStream o = new FileOutputStream(path);
        FileChannel fc = o.getChannel();


        fc.write(daScrivere);
        System.out.println("scrittura effettuata");


        path = "data/doc_index.bin";
        FileInputStream i = new FileInputStream(path);
        FileChannel leggo = i.getChannel();
        long dim=leggo.size();
        System.out.println("dim "+dim);
//        ByteBuffer b = ByteBuffer.allocate((int)dim);
//        leggo.read(b);
//        b.flip();
//
//        b.position((int)dim - 8);
//        System.out.println(b.getInt());
        ByteBuffer b = ByteBuffer.allocate(4);
        leggo.position(dim-12);
        leggo.read(b);
        b.flip();
        System.out.println(b.getInt());

    }

    @Test
    public void encoderTestBRUTTO(){
        VBEncoder vb=new VBEncoder();
        System.out.println(vb.encode(128000)[0]);
        System.out.println(vb.encode(128000)[1]);
        System.out.println(vb.encode(128000)[2]);

    }

    @Test
    public void VBencoderTest(){
        List<Integer> numeri= new ArrayList<>();
        numeri.add(100); //-28 //TODO sistemare usando assertequals
        numeri.add(2000000); //122 9 -128
        numeri.add(3); //-125
        VBEncoder p = new VBEncoder();
        ByteBuffer b = p.encodeList(numeri);
        while(b.hasRemaining()){
            System.out.println(b.get());
        }
        b.flip();
        System.out.println(p.decodeList(b));

    }

    @Test
    public void UnaryEncoderTest(){
        List<Integer> numeri= new ArrayList<>();
        numeri.add(10);
        numeri.add(20);
        numeri.add(3);
        UnaryEncoder p = new UnaryEncoder();
        ByteBuffer b = p.encodeList(numeri);
        while(b.hasRemaining()){
            System.out.println(b.get()); //-1 -65 -1 -5 127 //TODO sistemare usando assertequals
        }
        b.flip();
        System.out.println(p.decodeList(b));
    }


}