package edu.davidson.csc353.microdb.indexes.bptree;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.io.RandomAccessFile;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.function.Function;

import edu.davidson.csc353.microdb.utils.DecentPQ;

public class BPNodeFactory<K extends Comparable<K>, V> {
	public static final int DISK_SIZE = 512;

	public static final int CAPACITY = 15;

	private String indexName;

	private Function<String, K> loadKey;
	private Function<String, V> loadValue;

	private int numberNodes;

	private RandomAccessFile relationFile;
	private FileChannel relationChannel;

	private TreeMap<Integer, NodeTimestamp> nodeMap;
	
	private DecentPQ<NodeTimestamp> nodePQ;

	private class NodeTimestamp implements Comparable<NodeTimestamp> {
		public BPNode<K,V> node;
		public long lastUsed;

		public NodeTimestamp(BPNode<K,V> node, long lastUsed) {
			this.node = node;
			this.lastUsed = lastUsed;
		}

		public int compareTo(NodeTimestamp other) {
			return (int) (lastUsed - other.lastUsed);
		}
	}

	/**
	 * Creates a new NodeFactory object, which will operate a buffer manager for
	 * the nodes of a B+Tree.
	 * 
	 * @param indexName The name of the index stored on disk.
	 */
	public BPNodeFactory(String indexName, Function<String, K> loadKey, Function<String, V> loadValue) {
		try {
			this.indexName = indexName;
			this.loadKey = loadKey;
			this.loadValue = loadValue;

			Files.delete(Paths.get(indexName + ".db"));

			relationFile = new RandomAccessFile(indexName + ".db", "rws");
			relationChannel = relationFile.getChannel();

			numberNodes = 0;

			nodeMap = new TreeMap<Integer, NodeTimestamp>();
		}
		catch (FileNotFoundException exception) {
			// Ignore: a new file has been created
		}
		catch(IOException exception) {
			// throw new RuntimeException("Error accessing " + indexName);
		}
	}

	/**
	 * Creates a B+Tree node.
	 * 
	 * @param leaf Flag indicating whether the node is a leaf (true) or internal node (false)
	 * 
	 * @return A new B+Tree node.
	 */
	public BPNode<K,V> create(boolean leaf) {

		if(nodeMap.size() >= CAPACITY) {
			evict();
		}

		BPNode<K,V> createdNode = new BPNode<K,V>(leaf);
		createdNode.number = numberNodes;
		numberNodes++;

		NodeTimestamp newest = new NodeTimestamp(createdNode, System.nanoTime());
		nodeMap.put(createdNode.number, newest);
		nodePQ.add(newest);

		return createdNode;
	}

	/**
	 * Saves a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	public void save(BPNode<K,V> node) {
		writeNode(node);
	}

	/**
	 * Reads a node from the disk.
	 * 
	 * @param nodeNumber Number of the node read.
	 * 
	 * @return Node read from the disk that has the provided number.
	 */
	private BPNode<K,V> readNode(int nodeNumber) {

		ByteBuffer buffer = ByteBuffer.allocate(DISK_SIZE);
		// doesn't matter whether leaf if true/false. Will get overriden. 
		BPNode<K,V> node = new BPNode<K,V>(false);
		
		relationChannel.read(buffer, nodeNumber * DISK_SIZE);
		buffer.rewind();
		node.load(buffer, loadKey, loadValue);

		return node;
	}

	/**
	 * Writes a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	private void writeNode(BPNode<K,V> node) {
		ByteBuffer buffer = ByteBuffer.allocate(DISK_SIZE);

		node.save(buffer);
		buffer.rewind();
		relationChannel.write(buffer, node.number * DISK_SIZE);
	}

	/**
	 * Evicts the last recently used node back into disk.
	 */
	private void evict() {
		NodeTimestamp oldest = nodePQ.removeMin();
		nodeMap.remove(oldest.node.number);

		relation.writeNode(oldest.node);
	}

	/**
	 * Returns the node associated with a particular number.
	 * 
	 * @param number The number to be converted to node (loading it from disk, if necessary).
	 * 
	 * @return The node associated with the provided number.
	 */
	public BPNode<K,V> getNode(int number) {
		NodeTimestamp nodeTimestamp = nodeMap.get(number);

		// if node is in memory
		if (nodeTimestamp != null) {
			nodeTimestamp.lastUsed = System.nanoTime();
			nodePQ.increaseKey(nodeTimestamp);

			return nodeTimestamp.node;
		}
		// else, need to get node from disk, and also update timestamp/queue
		BPNode<K,V> diskNode = this.readNode(number);
		NodeTimestamp newest = new NodeTimestamp(diskNode, System.nanoTime());
		nodeMap.put(diskNode.number, newest);
		nodePQ.add(newest);

		return diskNode;
	}
}
