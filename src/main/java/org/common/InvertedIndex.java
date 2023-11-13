package org.common;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndex implements InvertedIndexInterface {

    // TODO e' necessario il serializable -> serialVersionUID
    private List<PostingList> inverted_index;

    public InvertedIndex() {
        this.inverted_index = new ArrayList<PostingList>();
    }

    @Override
    public void addPosting(int index, Posting posting){
        this.inverted_index.add(index, new PostingList(posting));
    }

    @Override
    public void appendPosting(int index, Posting posting){
        this.inverted_index.get(index).addPosting(posting);
    }


    public void addPostingList(int index, PostingList postingList){
        return;
    }

    public void appendPostingList(int index, PostingList postingList){
        this.inverted_index.get(index).appendPostings(postingList);
    }

    public int getSize(){
        return this.inverted_index.size();
    }

    public List<PostingList> getInverted_index() {
        return inverted_index;
    }

    @Override
    public String toString() {
        return "InvertedIndex{" + inverted_index + "}";
    }
}
