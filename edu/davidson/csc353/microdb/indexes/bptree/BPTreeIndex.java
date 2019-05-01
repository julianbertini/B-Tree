
package edu.davidson.csc353.microdb.indexes.bptree;

import java.util.ArrayList;
import java.util.function.Function;

import edu.davidson.csc353.microdb.files.Tuple;
import edu.davidson.csc353.microdb.files.Record;
import edu.davidson.csc353.microdb.files.Queriable;

import edu.davidson.csc353.microdb.indexes.RecordLocation;

import edu.davidson.csc353.microdb.indexes.SecondaryIndex;

public class BPTreeIndex<T extends Tuple, K extends Comparable<K>> implements SecondaryIndex<T, K> {
	private BPTree<K, RecordLocation> tree;
	
	public BPTreeIndex(Queriable<T> queriable, Function<T, K> keyExtractor, Function<String, K> loadKey) {
		//TODO: - create a BPTree<K, RecordLocation> called "tree"
		//      - make sure to pass the appropriate function to convert from a string "(x, y)" to a RecordLocation
		//        block number x and record number y
		//      - iterate over the queriable, and add the entries to the B+Tree
		
		tree = new BPTree<K, RecordLocation>(loadKey, s -> loadValue(s));
		
		for(Record<T> record: queriable){
			//extract k
			K key = keyExtractor.apply(record.getTuple());

			//insert into the tree
			tree.insert(key, new RecordLocation(record.getBlockNumber(), record.getRecordNumber()));
		}
	}
	
	private RecordLocation loadValue(String s){
		String numString = s.substring(1, s.length() - 1);
		String splitString[] = numString.split(",");
		int block = Integer.valueOf(splitString[0]);
		int record = Integer.valueOf(splitString[1].substring(1)); // get rid of whitespace
		RecordLocation location = new RecordLocation(block, record);
		return location; 
	}

	public Iterable<RecordLocation> get(K key) {
		ArrayList<RecordLocation> results = new ArrayList<>();

		RecordLocation location = tree.get(key);

		if(location != null) {
			results.add(location);
		}

		return results;
	}
}