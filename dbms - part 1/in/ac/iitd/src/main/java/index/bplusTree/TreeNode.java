package index.bplusTree;

import java.nio.ByteBuffer;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    default public byte [] convert_int_to_byte(int x){
        byte [] ans = new byte[2];
        ans[0] = (byte) ((x >> 8) & 0xFF);
        ans[1] = (byte) (x & 0xFF);
        return ans;
    }
    default public boolean greater_equal_to(T key1, T key2){
        if(key1 instanceof Boolean) {
            return ((Boolean) key1 | (Boolean) key1 == (Boolean) key2);
        }else if(key1 instanceof Integer){
            return ((Integer) key1 >= (Integer) key2);
        }else if (key1 instanceof Long) {
            return ((Long) key1 >= (Long) key2);
        } else if (key1 instanceof Float) {
            return ((Float) key1 >= (Float) key2);
        } else if (key1 instanceof Double) {
            return ((Double) key1 >= (Double) key2);
        }else if (key1 instanceof String) {
            return ((String) key1).compareTo((String) key2) >= 0;
        }
        throw new IllegalArgumentException("Wrong Argument Type");
    }

    
    // Might be useful for you - will not be evaluated
    default T convertBytesToT(byte[] bytes, Class<T> typeClass) {
        if (typeClass == Integer.class) {
            return (T) Integer.valueOf(ByteBuffer.wrap(bytes).getInt());
        } else if (typeClass == Long.class) {
            return (T) Long.valueOf(ByteBuffer.wrap(bytes).getLong());
        } else if (typeClass == Float.class) {
            return (T) Float.valueOf(ByteBuffer.wrap(bytes).getFloat());
        } else if (typeClass == Double.class) {
            return (T) Double.valueOf(ByteBuffer.wrap(bytes).getDouble());
        } else if (typeClass == Boolean.class) {
            return (T) Boolean.valueOf(bytes[0] != 0);
        } else if (typeClass == String.class) {
            return (T) new String(bytes);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + typeClass.getName());
        }
    }

    default byte[] convertTToBytes(T object) {
        if (object instanceof Integer) {
            return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) object).array();
        } else if (object instanceof Long) {
            return ByteBuffer.allocate(Long.BYTES).putLong((Long) object).array();
        } else if (object instanceof Float) {
            return ByteBuffer.allocate(Float.BYTES).putFloat((Float) object).array();
        } else if (object instanceof Double) {
            return ByteBuffer.allocate(Double.BYTES).putDouble((Double) object).array();
        } else if (object instanceof Boolean) {
            return new byte[]{(byte) (((Boolean) object) ? 1 : 0)};
        } else if (object instanceof String) {
            return ((String) object).getBytes();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + object.getClass().getName());
        }
    }

}