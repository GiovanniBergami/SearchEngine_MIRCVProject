package org.common;

import org.common.encoding.EncoderInterface;
import org.offline_phase.Spimi;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

public class ChunkHandler {

    public static final String basename = "data/";
    public static final String basename_intermediate_index = "data/intermediate_postings/index/";
    public static final String basename_intermediate_lexicon = "data/intermediate_postings/lexicon/";
    public static final String basename_intermediate_docindex = "data/intermediate_postings/doc_index/";
    static Logger logger = Logger.getLogger(Spimi.class.getName());
    private static EncoderInterface encoder;

    public static void setEncoder(EncoderInterface e){
        encoder = e;
    }

    public static void writeLexicon(Lexicon lexicon, String lexicon_filename, boolean intermediate){

        try (FileOutputStream indexFileOutputStream = new FileOutputStream(lexicon_filename, false);
             FileChannel indexFileChannel = indexFileOutputStream.getChannel()) {

            for(String k : lexicon.keySet())
                indexFileChannel.write(lexicon.serializeEntry(k));

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Lexicon " + lexicon_filename + " written on disk");
    }

    public static TermEntry writePostingList(FileChannel indexFileChannel, PostingList postingList, boolean intermediate) throws IOException {

        // to save the starting position
        long startPosition;
        long length;

        long pointerFilePosition;
        long blockStartPosition;

        startPosition = indexFileChannel.position();

        if(!intermediate){
            int i = 0;
            /*
                TODO - add postingList.generateSkipping() here or back ?
             */
            for (SkippingPointer pointer : postingList.getSkipping_points()) {

                // where the skipping pointer must be written
                pointerFilePosition = indexFileChannel.position();

                // reserve the space for the Skipping Pointer
                indexFileChannel.position(pointerFilePosition + SkippingPointer.SIZE);

                blockStartPosition = indexFileChannel.position();
                while (i < postingList.getSize() && pointer.getMax_doc_id() >= postingList.getPosting(i).getDoc_id()) {
                    Posting posting = postingList.getPostingList().get(i);
                    indexFileChannel.write(posting.serialize(encoder));
                    i++;
                }
                pointer.setOffset((short) (indexFileChannel.position() - blockStartPosition));
                indexFileChannel.write(pointer.serialize(), pointerFilePosition);
            }
        }else{
            for(Posting posting : postingList){
                indexFileChannel.write(posting.serialize(encoder));
            }
        }
        length = indexFileChannel.position() - startPosition;

        //System.out.println(postingList);
        return new TermEntry(-1, startPosition, length);
    }


    public static Lexicon readLexicon(String lexicon_filename){

        Lexicon lexicon = null;
        try (FileInputStream lexiconFileInputStream = new FileInputStream(lexicon_filename);
             FileChannel indexFileChannel = lexiconFileInputStream.getChannel()) {

            long size = indexFileChannel.size();
            ByteBuffer buffer = ByteBuffer.allocate((int) size);
            indexFileChannel.read(buffer);
            buffer.flip();

            lexicon = new Lexicon(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Block " + lexicon_filename + " has been read from disk");
        return lexicon;
    }

    public static PostingList readPostingList(TermEntry termEntry, boolean intermediate){

        // infer filename
        String index_filename;
        if(intermediate)
             index_filename = String.format(basename_intermediate_index + "block_index_%05d.bin", termEntry.getBlock_index());
        else
            index_filename = basename + "index.bin";

        try (FileInputStream indexFileInputStream = new FileInputStream(index_filename);
             FileChannel indexFileChannel = indexFileInputStream.getChannel()) {

            // Read bytes into ByteBuffer
            ByteBuffer indexByteBuffer = ByteBuffer.allocate((int) termEntry.getLength());
            indexFileChannel.position(termEntry.getOffset());
            indexFileChannel.read(indexByteBuffer);
            indexByteBuffer.flip();

            return new PostingList(indexByteBuffer, encoder, !intermediate);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static PostingList readPostingList(FileChannel channel, TermEntry termEntry, boolean intermediate) throws IOException {

        ByteBuffer indexByteBuffer = ByteBuffer.allocate((int) termEntry.getLength());
        channel.position(termEntry.getOffset());
        channel.read(indexByteBuffer);
        indexByteBuffer.flip();

        return new PostingList(indexByteBuffer, encoder, !intermediate);
    }


    public static void writeDocIndex(DocIndex docIndex, String doc_index_filename){

        try (FileOutputStream docIndexFileOutputStream = new FileOutputStream(doc_index_filename, false);
             FileChannel docIndexFileChannel = docIndexFileOutputStream.getChannel()) {

            docIndexFileChannel.write(docIndex.serialize());

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Lexicon " + doc_index_filename + " written on disk");


    }

    public static DocIndex readDocIndex(String doc_index_filename){

        DocIndex doc_index = null;

        try (FileInputStream docIndexFileInputStream = new FileInputStream(doc_index_filename);
             FileChannel docIndexFileChannel = docIndexFileInputStream.getChannel()) {

            long size = docIndexFileChannel.size();
            ByteBuffer buffer = ByteBuffer.allocate((int) size);
            docIndexFileChannel.read(buffer);
            buffer.flip();

            doc_index = new DocIndex(buffer);
        } catch (IOException e) {
                e.printStackTrace();
        }

        logger.info("Block " + doc_index_filename + " has been read from disk");
        return doc_index;
    }


}