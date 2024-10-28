
package index.bplusTree;

import storage.AbstractFile;

import javax.xml.soap.Node;
import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Vector;

/*
 * Tree is a collection of BlockNodes
 * The first BlockNode is the metadata block - stores the order and the block_id of the root node

 * The total number of keys in all leaf nodes is the total number of records in the records file.
 */

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;
    public Class give_type(){
        return typeClass;
    }

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return ((rootBlockIdBytes[0] & 0xFF) <<8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return ((orderBytes[0] << 8) & 0xF0) | (orderBytes[1] & 0x0F);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id) {
        return isLeaf(blocks.get(id));
    }
    private void insert_in_parent(int left_child_id,T key,  int right_child_id, Vector<Integer> baap, int len_v){
        if(baap.get(len_v)==-1){
            InternalNode<T> root_v = new InternalNode<>( key, left_child_id, right_child_id, typeClass);
            blocks.add(root_v);
            int newRootId = blocks.size()-1;
            byte[] rootBytes_v = root_v.convert_int_to_byte(newRootId);
            blocks.get(0).write_data(2, rootBytes_v);
            return;
        }

        int parent_v_id = baap.get(len_v);
        InternalNode<T> parent_v = (InternalNode<T>) blocks.get(parent_v_id);

        if(!isFull(parent_v_id)){
            parent_v.insert(key,right_child_id);
            return;
        }

        parent_v.insert(key,right_child_id);

        LeafNode<T> parent_one_rc = new LeafNode<>(typeClass);
        blocks.add(parent_one_rc);

        int parent_numKeys = parent_v.getNumKeys();
        T[] parent_keys = parent_v.getKeys();
        int[] children_v = parent_v.getChildren();


        int hello=(parent_numKeys-1)/2;
        InternalNode<T> parent_one = new InternalNode<>(parent_keys[hello-1],children_v[0],children_v[hello],typeClass);
        InternalNode<T> parent_two = new InternalNode<>(parent_keys[hello+1],children_v[hello+1],children_v[hello+2],typeClass);

        for(int i=0;i<hello-1;i++) {
            parent_one.insert(parent_keys[i],children_v[i+1]);
        }
        for(int i=hello+2;i<parent_numKeys;i++){
            parent_two.insert(parent_keys[i],children_v[i+1]);
        }

        blocks.set(parent_v_id,parent_one);
        blocks.add(parent_two);
        len_v--;
        insert_in_parent(parent_v_id,parent_keys[hello],blocks.size()-1,baap,len_v);
    }
    // will be evaluated
    public void insert(T key, int block_id) {

        // Vidu Begins
        if(blocks.size()==1) {
            LeafNode<T> leafNode = new LeafNode<>(typeClass);
            leafNode.insert(key,block_id);
            blocks.add(leafNode);

            byte[] rootIdBytes = new byte[2];
            rootIdBytes[0] = 0;
            rootIdBytes[1] = 1;
            blocks.get(0).write_data(2, rootIdBytes);
            return;
        }
//        Find the leaf node that should contain key value
        int rootId = getRootId();
        BlockNode rootNode = blocks.get(rootId);

        Vector<Integer> baap = new Vector<Integer>();
        baap.add(-1);
        int childId = rootId;

        while (true) {
            if (isLeaf(rootNode)) {
                break;
            } else {
                InternalNode<T> internalNode = (InternalNode<T>) rootNode;
                baap.add(childId);
                childId = internalNode.search(key);
                rootNode = blocks.get(childId);
            }
        }

        LeafNode<T> final_leaf = (LeafNode<T>) blocks.get(childId);
        if(!isFull(childId)){
            (final_leaf).insert(key,block_id);
            return;
        }

//      If the leaf is full
        int numKeys = final_leaf.getNumKeys();
        final_leaf.insert(key,block_id);
        LeafNode<T> leaf_one = new LeafNode<>(typeClass);
        LeafNode<T> leaf_two = new LeafNode<>(typeClass);

        numKeys++;
        T[] keys = final_leaf.getKeys();
        int[] blocks_v = final_leaf.getBlockIds();
        blocks.set(childId,leaf_one);
        blocks.add(leaf_two);
        int leaf_one_id=childId,leaf_two_id = blocks.size()-1;


        for(int i=0;i<(numKeys-1)/2;i++) {
            leaf_one.insert(keys[i],blocks_v[i]);
        }
        for(int i=(numKeys-1)/2;i<numKeys;i++) {
            leaf_two.insert(keys[i], blocks_v[i]);
        }

        int len_v = baap.size()-1;

        //        ????????????????Confused
        byte[] baap_ka_right = final_leaf.get_data(4,2);
        byte[] baap_ka_left = final_leaf.get_data(2,2);
        leaf_two.write_data(4,baap_ka_right);
        leaf_one.write_data(4, leaf_one.convert_int_to_byte(leaf_two_id));
        leaf_two.write_data(2,leaf_two.convert_int_to_byte(leaf_one_id));
        leaf_one.write_data(2,baap_ka_left);

        insert_in_parent(childId,keys[numKeys/2],leaf_two_id,baap,len_v);

        // Vidu Ends
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key

    public int search(T key) {

        /* Write your code here */
        // Vidu Begins
        // Confused about what to return here
        int rootId = getRootId();
        BlockNode rootNode = blocks.get(rootId);
        int childId = rootId;

        while (true) {
            if (isLeaf(rootNode)) {
                if (((LeafNode<T>) rootNode).search(key)>-1) return childId;
                return -1;
            } else {
                InternalNode<T> internalNode = (InternalNode<T>) rootNode;
                childId = internalNode.search(key); // Search for the appropriate child
                rootNode = blocks.get(childId); // Move to the child node
            }
        }
        // Vidu Ends
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

    public int search_for_geq(T key) {

        /* Write your code here */
        int rootId = getRootId();
        int childId = rootId;
        BlockNode rootNode = blocks.get(rootId);

        while (true) {
            if (isLeaf(rootNode)) {
                break;
            } else {
                InternalNode<T> internalNode = (InternalNode<T>) rootNode;
                childId = internalNode.search(key); // Search for the appropriate child
                rootNode = blocks.get(childId); // Move to the child node
            }
        }
        int leaf_node_id = childId;
        LeafNode<T> leafNode = new LeafNode<>(typeClass);
//        int [] ret_arr = new int[2];
//        ret_arr[0]=-1
//        ret_arr[1]=-1;
        while(true){
            if(leaf_node_id==0||leaf_node_id==-1) return -1;
            leafNode = (LeafNode<T>) blocks.get(leaf_node_id);
            int numKeys = leafNode.getNumKeys();
            T[] keys = leafNode.getKeys();
            for(int i=0;i<numKeys;i++){
                if(leafNode.greater_equal_to(keys[i],key)){
//                    ret_arr[0]=leaf_node_id;
//                    int[] blocks= leafNode.getBlockIds();
//                    ret_arr[1] = i;
                    return leaf_node_id;
                }
            }
            byte[] next_leaf = leafNode.get_data(4,2);
            leaf_node_id=((next_leaf[0] << 8) & 0xFF) | (next_leaf[1] & 0xFF);
        }
        // Vidu Ends
    }
    public ArrayList<Integer> greater_block_list(T key){
        ArrayList<Integer> block_l= new ArrayList<>();
        int leaf_node_id = search_for_geq(key);
        if(leaf_node_id==-1) return block_l;
        LeafNode<T> leafNode;
        while(true){
            if(leaf_node_id==0||leaf_node_id==-1) return block_l;
            leafNode = (LeafNode<T>) blocks.get(leaf_node_id);
            int numKeys = leafNode.getNumKeys();
            T[] keys = leafNode.getKeys();
            int[] block_ids = leafNode.getBlockIds();
            for(int i=0;i<numKeys;i++){
                if(leafNode.greater_equal_to(keys[i],key) && !key.equals(keys[i])){
//                    System.out.println("oh"+block_ids[i]);
                    block_l.add(block_ids[i]);
                }
            }
            byte[] next_leaf = leafNode.get_data(4,2);
            leaf_node_id=((next_leaf[0] << 8) & 0xFF) | (next_leaf[1] & 0xFF);
        }

    }


    public ArrayList<Integer> less_block_list(T key){
        ArrayList<Integer> block_l= new ArrayList<>();
        int leaf_node_id = search_for_geq(key);
        if(leaf_node_id==-1) {
            int rid = getRootId();
            while (!isLeaf(rid)){
                InternalNode<T> internalNode = (InternalNode<T>) blocks.get(rid);
                int[] children = internalNode.getChildren();
                rid = children[0];
            }

            leaf_node_id = rid;
            LeafNode<T> leafNode = new LeafNode<>(typeClass);
            while(true){
                if(leaf_node_id==0||leaf_node_id==-1) return block_l;
                leafNode = (LeafNode<T>) blocks.get(leaf_node_id);
                int numKeys = leafNode.getNumKeys();
                T[] keys = leafNode.getKeys();
                int[] block_ids = leafNode.getBlockIds();
                for(int i=0;i<numKeys;i++){
                    if(!leafNode.greater_equal_to(keys[i],key)){
//                    System.out.println("oh"+block_ids[i]);
                        block_l.add(block_ids[i]);
                    }
                }
                byte[] prev_leaf = leafNode.get_data(4,2);
                leaf_node_id=((prev_leaf[0] << 8) & 0xFF) | (prev_leaf[1] & 0xFF);
            }
        }
        LeafNode<T> leafNode;
        while(true){
            if(leaf_node_id==0||leaf_node_id==-1) return reverse_list(block_l);
            leafNode = (LeafNode<T>) blocks.get(leaf_node_id);
            int numKeys = leafNode.getNumKeys();
            T[] keys = leafNode.getKeys();
            int[] block_ids = leafNode.getBlockIds();
            for(int i=numKeys-1;i>=0;i--){
                if(!leafNode.greater_equal_to(keys[i],key)){
//                    System.out.println("oh"+block_ids[i]);
                    block_l.add(block_ids[i]);
                }
            }
            byte[] prev_leaf = leafNode.get_data(2,2);
            leaf_node_id=((prev_leaf[0]  & 0xFF) << 8) | (prev_leaf[1] & 0xFF);
        }

    }
    public int go_back(int leaf_node_id,T key){
        LeafNode<T> leafNode;
        int prev_node_id;
        while(true){
            leafNode = (LeafNode<T>) blocks.get(leaf_node_id);
            int numKeys = leafNode.getNumKeys();
            T[] keys = leafNode.getKeys();
            if(!key.equals(keys[0])) return leaf_node_id;
            byte[] next_leaf = leafNode.get_data(2, 2);
            prev_node_id = ((next_leaf[0] & 0xFF) <<8 ) | (next_leaf[1] & 0xFF);
            if (prev_node_id == 0 || prev_node_id == -1) return leaf_node_id;
            leaf_node_id=prev_node_id;
        }
    }
    public ArrayList<Integer> equal_block_list(T key) {
        ArrayList<Integer> block_l= new ArrayList<>();
        int leaf_node_id = search_for_geq(key);
        if(leaf_node_id==-1) return block_l;
        LeafNode<T> leafNode;
        leaf_node_id=go_back(leaf_node_id,key);
        while(true) {
            if (leaf_node_id == 0 || leaf_node_id == -1) return block_l;
            leafNode = (LeafNode<T>) blocks.get(leaf_node_id);
            int numKeys = leafNode.getNumKeys();
            T[] keys = leafNode.getKeys();
            int[] block_ids = leafNode.getBlockIds();
            for (int i = 0; i < numKeys; i++) {
                if (key.equals(keys[i])) {
                    block_l.add(block_ids[i]);
                }
            }
            byte[] next_leaf = leafNode.get_data(4, 2);
            leaf_node_id = ((next_leaf[0] & 0xFF) <<8 ) | (next_leaf[1] & 0xFF);
        }
    }
    public ArrayList<Integer> geq_block_list(T key){
        ArrayList<Integer> block_l= equal_block_list(key);
        ArrayList<Integer> block_l2= greater_block_list(key);
        for(int i=0;i<block_l2.size();i++){
            block_l.add(block_l2.get(i));
        }
        return block_l;
    }
    public ArrayList<Integer> leq_block_list(T key){
        ArrayList<Integer> block_l2= equal_block_list(key);
        ArrayList<Integer> block_l= less_block_list(key);
        for(int i=0;i<block_l2.size();i++){
            block_l.add(block_l2.get(i));
        }
        return block_l;
    }
    public ArrayList<Integer> reverse_list(ArrayList<Integer> hey){
        ArrayList<Integer> ret_list = new ArrayList<Integer>();
        for(int i=hey.size()-1;i>=0;i--){
            ret_list.add(hey.get(i));
        }
        return ret_list;
    }

}

	
