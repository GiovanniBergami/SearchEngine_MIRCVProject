package org.offline_phase;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.common.*;
import org.common.encoding.NoEncoder;
import org.common.encoding.UnaryEncoder;
import org.common.encoding.VBEncoder;
import org.online_phase.scoring.BM25;
import org.online_phase.scoring.TFIDF;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * The Spimi (Single-Pass In-Memory Indexing) processes a collection of documents,
 * tokenizes and parses them, creates intermediate index structures and merges them
 * to create a final  index for information retrieval.
 * In order to speed up computation, multithreading has been used to parallelize
 * the processing of document chunks.
 */
public class Spimi {

    private int doc_id_counter = 0;     // doc_id counter, useful for generating filenames
    private int chunk_id_counter = 0;   // chunk counter, useful for generating filenames
    static final int  CHUNK_SIZE = 10000;   // number of documents for each chunk. BE CAREFUL, IT DEPENDS ON YOUR MACHINE
    static Logger logger = Logger.getLogger(Spimi.class.getName());
    private final boolean process_data_flag;    // true if stopword removal and stemming must be applied


    /**
     * Constructs a new Spimi instance with the specified configuration flags.
     *
     * @param process_data_flag     Flag indicating whether to apply stemming and stopwords removal.
     * @param compress_data_flag    Flag indicating whether to compress the final index.
     */
    public Spimi(boolean process_data_flag, boolean compress_data_flag) {
        this.process_data_flag = process_data_flag;

        if(compress_data_flag)
            PostingListReader.setEncoder(new VBEncoder(), new UnaryEncoder());
        else
            PostingListReader.setEncoder(new NoEncoder(), new NoEncoder());

    }

    /**
     * Processes a collection of documents, tokenizes and parses them, and creates
     * intermediate index structures using a multithreaded approach.
     *
     * @param collection_filepath   The file path to the collection of documents.
     */
    public void run(String collection_filepath){

        ExecutorService threadpool = Executors.newFixedThreadPool(10);

        // read the collection.tar.gz in a buffered way
        try(FileInputStream inputStream = new FileInputStream(collection_filepath);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            GzipCompressorInputStream gzipCompressorInputStream = new GzipCompressorInputStream(bufferedInputStream);
            TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzipCompressorInputStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8))){

            tarArchiveInputStream.getNextTarEntry();

            // initialize content parser
            ContentParser parser = new ContentParser("data/stop_words_english.txt", this.process_data_flag);
            String line;
            do{
                // chunk of CHUNK_SIZE documents
                StringBuilder chunk_text = new StringBuilder();
                do{
                    line = br.readLine();

                    // append lines to the chunks
                    if ( line != null && !line.isEmpty()) {
                        chunk_text.append(line).append("\n");
                        doc_id_counter++;
                    }
                }while(doc_id_counter < CHUNK_SIZE * (chunk_id_counter+1) && line != null);

                System.gc();
                // wait if the free memory is less than 20% to avoid OutOfMemoryError
                while(Runtime.getRuntime().freeMemory() < (Runtime.getRuntime().totalMemory() * 20 / 100)) {
                    System.gc();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                    }
                }

                // give each chunk to a thread
                threadpool.submit(new ProcessChunkThread(chunk_text, chunk_id_counter, parser));

                chunk_id_counter++;
            }while (line != null);

            // wait all threads to finish
            threadpool.shutdown();
            try{
                threadpool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                e.printStackTrace();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Merges the intermediate index structures to create a final index for
     * information retrieval. This includes merging lexicons, DocIndex, and index files.
     */
    public void merge_chunks(){

        Lexicon merged_lexicon = new Lexicon();
        Lexicon current_lexicon;

        File lexicon_directory = new File(LexiconReader.basename_intermediate_lexicon);

        File[] lexicon_files = lexicon_directory.listFiles();

        long start_time = System.currentTimeMillis();
        // merge all intermediate lexicon to create a unique one
        for (File lexiconFile : lexicon_files) {
            current_lexicon = LexiconReader.readLexicon(LexiconReader.basename_intermediate_lexicon + lexiconFile.getName());
            merged_lexicon.merge(current_lexicon);
        }
        logger.info("Intermediate Lexicons merged in " + (System.currentTimeMillis() - start_time)/1000.0 + "s");


        // merge all intermediate DocIndex to create a unique one
        File docindex_directory = new File(DocIndexReader.basename_intermediate_docindex);
        File[] docindex_files = docindex_directory.listFiles();
        start_time = System.currentTimeMillis();
        try (FileOutputStream indexFileOutputStream = new FileOutputStream(DocIndexReader.basename + "doc_index.bin", false);
             FileChannel docIndexFileChannel = indexFileOutputStream.getChannel()) {

            for(File docIndex_file : docindex_files){

                try (FileInputStream indexFileInputStream = new FileInputStream(DocIndexReader.basename_intermediate_docindex + docIndex_file.getName());
                     FileChannel docIndex_intermediate_FileChannel = indexFileInputStream.getChannel()) {

                    long size = docIndex_intermediate_FileChannel.size();
                    ByteBuffer buffer = ByteBuffer.allocate((int) size);
                    docIndex_intermediate_FileChannel.read(buffer);
                    buffer.flip();
                    // we don't instantiate a docIndex object, but we just copy-paste all intermediate docIndexes into
                    // the final docIndex.bin file
                    docIndexFileChannel.write(buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Intermediate DocIndex merged in " + (System.currentTimeMillis() - start_time)/1000.0 + "s");


        // merge all intermediate indexes
        start_time = System.currentTimeMillis();
        try (FileOutputStream indexFileOutputStream = new FileOutputStream("data/index.bin", false);
             FileChannel indexFileChannel = indexFileOutputStream.getChannel()) {

            // open a file channel for each intermediate index file
            ArrayList<FileInputStream> fileInputStreams = new ArrayList<>();
            File index_directory = new File(PostingListReader.basename_intermediate_index);
            File[] indexFiles = index_directory.listFiles();
            for(File indexFile : indexFiles){
                fileInputStreams.add(new FileInputStream(PostingListReader.basename_intermediate_index + indexFile.getName()));
            }

            ArrayList<FileChannel> fileChannels = new ArrayList<>();
            for(FileInputStream fos : fileInputStreams)
                fileChannels.add(fos.getChannel());

            BM25 bm25 = new BM25(DocIndexReader.basename_docindex);
            TFIDF tfidf = new TFIDF(DocIndexReader.basename_docindex);

            TermEntry finalTermEntry;
            int i = 0;
            PostingList postingList;
            for(String term : merged_lexicon.keySet()){

                // get a list of pointers to different intermediate index files
                TermEntryList termEntryList = merged_lexicon.get(term);
                termEntryList.setTerm_index(i);

                // the final posting for a given term
                postingList = new PostingList();

                // each term has a list of pointers to different intermediate index files
                for(TermEntry termEntry : termEntryList){
                    PostingList p = PostingListReader.readIntermediatePostingList(fileChannels.get(termEntry.getBlock_index()), termEntry);
                    postingList.appendPostings(p);
                }

                // generate skipping pointers on the final posting list
                postingList.initPointers();
                // write the final posting list
                finalTermEntry = PostingListReader.writePostingList(indexFileChannel, postingList);

                // compute both TFIDF and BM25 term upper bound for the given term
                finalTermEntry.setTfidf_upper_bound(tfidf.getTermUpperBound(postingList));
                finalTermEntry.setBm25_upper_bound(bm25.getTermUpperBound(postingList));

                // update the termEntry information
                merged_lexicon.get(term).resetTermEntry(finalTermEntry);
                i++;
            }

            // close all file channes
            for(FileChannel fc : fileChannels)
                fc.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Intermediate Index merged in " + (System.currentTimeMillis() - start_time)/1000.0 + "s");
        // write lexicon to disk
        LexiconReader.writeLexicon(merged_lexicon, LexiconReader.basename + "lexicon.bin");
    }

}
