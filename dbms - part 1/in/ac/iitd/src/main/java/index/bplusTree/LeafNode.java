package index.bplusTree;


/*
 * A LeafNode contains keys and block ids.
 * Looks Like -
 * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
 *
 * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        // Vidu Begins
        // What is the size of BlockID -> assumed 2 -> mp it is
        int offset=10;
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
    public void print_node(){
        int numKeys = getNumKeys();
        int offset = 8;
        System.out.println("numkeys " + numKeys);
        for (int i = 0; i < numKeys; i++) {
            int block_id = this.get_data_int_len_2(offset);
            System.out.println("The block id is");
            System.out.println(block_id);
            offset+=2 ; // Move offset after block id
            int keyLength = this.get_data_int_len_2(offset) ; // Read key length
            // print key length also
            System.out.println("The key length is");
            System.out.println(keyLength);
            offset += 2; // Move offset after key length
            byte[] keyBytes = this.get_data(offset, keyLength); // Read key bytes
            offset += keyLength; // Move offset after key
            T currentKey = convertBytesToT(keyBytes, typeClass); // Convert bytes to key
            System.out.println("The key is");
            System.out.println(currentKey);
        }
    }
    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        // Vidu Begins
        int offset = 8;
        for (int i = 0; i < numKeys; i++) {
            byte[] blockIdBytes = this.get_data(offset, 2);
            block_ids[i] = ((blockIdBytes[0] << 8) & 0xFF00 | (blockIdBytes[1] & 0xFF));
            offset += 2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) & 0xFF00) | (keyLenBytes[1] & 0xFF);
            offset+=keyLen+2;
        }
        // Vidu Ends

        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {
        /* Write your code here */
        // Vidu Begins
        int numKeys = getNumKeys();
        T[] keys = getKeys();
        int[] blocks_v = getBlockIds();

        int insertIndex = 0;
        int offset=8;
        while (insertIndex < numKeys && greater_equal_to(key,keys[insertIndex])) {
            offset+=2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            int keyLen = ((keyLenBytes[0] << 8) & 0xFF00) | (keyLenBytes[1] & 0xFF);
            offset+=keyLen+2;
            insertIndex++;
        }
//        offset=8;
        while(insertIndex<=numKeys){
            byte[] keyBytes = convertTToBytes(key);
            byte[] blockBytes = convert_int_to_byte(block_id);
            byte[] keyLenBytes = convert_int_to_byte(keyBytes.length);
//            print_node();
            this.write_data(offset,blockBytes);
            offset+=2;
            this.write_data(offset,keyLenBytes);
            offset+=2;
            this.write_data(offset,keyBytes);
            offset+=keyBytes.length;

            if(insertIndex<numKeys){
                key=keys[insertIndex];
                block_id=blocks_v[insertIndex];
            }
            insertIndex++;
//            print_node();
        }
        byte[] offsetBytes = convert_int_to_byte(offset);
        this.write_data(6,offsetBytes);
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
        int[] blocks_v = getBlockIds();

        for(int i=0;i<numKeys;i++){
            if(greater_equal_to(key,keys[i]) && greater_equal_to(keys[i],key)){
                return blocks_v[i];
            }
        }
        // Vidu Ends

        return -1;
    }

}
