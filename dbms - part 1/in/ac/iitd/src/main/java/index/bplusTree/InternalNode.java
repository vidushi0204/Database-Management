package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }
    public void print_node(){
        int numKeys = getNumKeys();
        int offset = 4;
        System.out.println("Number of keys: " + numKeys);
        for (int i = 0; i <= numKeys; i++) {
            System.out.println("Block id: " + this.get_data_int_len_2(offset));
            offset += 2;
            if (i < numKeys) {
                int keyLength = this.get_data_int_len_2(offset);
                System.out.println("Key length: " + keyLength) ;
                offset += 2;
                byte[] keyBytes = this.get_data(offset,keyLength);
                T key = convertBytesToT(keyBytes, typeClass);
                System.out.println("Key: " + key);
                offset += keyLength ;
            }
        }
    }
    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];
        /* Write your code here */
        // Vidu Begins
        int offset=6;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) & 0xFF00) | (keyLenBytes[1] & 0xFF);
            offset+=2;
            // Read key
            byte[] keyBytes = this.get_data(offset, keyLen);
            keys[i] = convertBytesToT(keyBytes, typeClass);
            offset+=keyLen+2;
        }
        // Vidu Ends
        return keys;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        // Vidu Begins
        int numKeys = getNumKeys();
        T[] keys = getKeys();
        int[] blocks_v = getChildren();

        int insertIndex = 0;
        int offset=4;
        while (insertIndex < numKeys && greater_equal_to(key,keys[insertIndex])) {
            offset+=2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) & 0xFF00) | (keyLenBytes[1] & 0xFF);
            offset+=keyLen+2;
            insertIndex++;
        }
        
        while(insertIndex<=numKeys){
            byte[] keyBytes = convertTToBytes(key);
            byte[] blockBytes = convert_int_to_byte(right_block_id);
            byte[] keyLenBytes = convert_int_to_byte(keyBytes.length);

            offset+=2;
            this.write_data(offset,keyLenBytes);
            offset+=2;
            this.write_data(offset,keyBytes);
            offset+=keyBytes.length;
            this.write_data(offset,blockBytes);

            if(insertIndex<numKeys){
                key=keys[insertIndex];
                right_block_id=blocks_v[insertIndex+1];
            }
            insertIndex++;
        }
        offset+=2;
        byte[] offsetBytes = convert_int_to_byte(offset);
        this.write_data(2,offsetBytes);
        numKeys++ ;
        byte [] numKeysBytes = convert_int_to_byte(numKeys) ;
        this.write_data(0,numKeysBytes);
        // Vidu Ends
        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        // Vidu Begins
        // Might wanna try binary search
        int numKeys = getNumKeys();
        T[] keys = getKeys();
        int[] children = getChildren();
        
        for(int i=0;i<numKeys;i++){
            if(!(greater_equal_to(key,keys[i]))){
                return children[i];
            }
        }
        return children[numKeys];
        // Vidu Ends
    }
    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        // Vidu Begins
        int offset = 4; 
        for (int i = 0; i <= numKeys; i++) {
            byte[] childIdBytes = this.get_data(offset, 2);
            children[i] = ((childIdBytes[0] << 8) & 0xFF00) | (childIdBytes[1] & 0xFF);
            offset += 2; 
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) & 0xFF00) | (keyLenBytes[1] & 0xFF);
            offset+=2+keyLen;
        }
        // Vidu Ends

        return children;

    }
//    public void Empty_Node(){
//        byte[] mk= convert_int_to_byte(6);
//        write_data(2,mk);
//        int offset=6;
//        byte[] keyLenBytes = this.get_data(6, 2);
//        int keyLen = ((keyLenBytes[0] << 8) & 0xFF00) | (keyLenBytes[1] & 0xFF);
//        byte[] numKeysBytes_vv = new byte[keyLen+2];
//        for(int i=0;i<keyLen;i++){
//            numKeysBytes_vv[i] = 0;
//        }
//
//        write_data(6,numKeysBytes_vv);
//    }

}
