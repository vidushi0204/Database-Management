package manager;

import index.bplusTree.BPlusTreeIndexFile;
import org.apache.calcite.avatica.proto.Common;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        if (!check_file_exists(table_name)|| block_id<0) {
            return null;
        }
        byte[] blockData = get_data_block(table_name, block_id);
        if (blockData == null) {
            return null;
        }
        byte[] schema_v = get_data_block(table_name, 0);

        int num_col = ((schema_v[1] & 0xFF) << 8) | (schema_v[0] & 0xFF);
        int offset_schema=2;
        int fixed_dt_count=0,var_dt_count=0;
        int curr_off_v;

        List<Integer> fixed_dt = new ArrayList<>();

        for(int i=0;i<num_col;i++){
            curr_off_v=((schema_v[offset_schema+1] & 0xFF) << 8) | (schema_v[offset_schema] & 0xFF);
            int dt=(schema_v[curr_off_v] & 0xFF);
            if(dt!=0) {
                fixed_dt_count++;
                fixed_dt.add(dt);
            }else{
                var_dt_count++;
            }
            offset_schema+=2;
        }

        int num_rec = ((blockData[0] & 0xFF) << 8) | (blockData[1] & 0xFF);
        int offset=2;
        int curr_off,prev_off=blockData.length;

//        byte [] printing_data = new byte[13];
//        System.out.println("Printing array");
//        System.out.println(blockData.length);
//        System.arraycopy(blockData, 4083, printing_data, 0, blockData.length - 4083);

//        for(int j=0;j<12;j++){
//            System.out.print(printing_data[j]+" ");
//        }


        List<Object[]> records = new ArrayList<>();
        for(int i=0;i<num_rec;i++){
            curr_off=((blockData[offset] & 0xFF) << 8 ) | ((blockData[offset+1] & 0xFF) ); // ye line chodi gyi hai
            Object[] record = extract_record(blockData,curr_off,fixed_dt,fixed_dt_count,var_dt_count);
            records.add(record);
            offset+=2;
        }
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        return records;
    }

//    public Object[] extract_record(byte[] blockData , int record_start , int record_end , int fixed_dt_count , int var_dt_count , List<Integer> fixed_dt) {
//        Object[] record = new Object[fixed_dt_count + var_dt_count];
//        int offset = 4 * var_dt_count;
//        int leng_v;
//        for (int i = 0; i < fixed_dt_count; i++) {
//            int dt = fixed_dt.get(i);
//            if (dt == 1) {
//                leng_v = 4;
//            } else if (dt == 2) {
//                leng_v = 1;
//            } else if (dt == 3) {
//                leng_v = 4;
//            } else {
//                leng_v = 8;
//            }
//            byte[] rec_bytes = new byte[leng_v];
//            System.arraycopy(blockData, record_start + offset, rec_bytes, 0, leng_v);
//    }


    private Object[] extract_record(byte[] blockData,int record_off,List<Integer> fixed_dt,int fixed_dt_count,int var_dt_count) {
        // Parse the record bytes according to the schema
        Object[] record = new Object[fixed_dt_count + var_dt_count];
        int offset = 4 * var_dt_count;
        int leng_v;
        for (int i = 0; i < fixed_dt_count; i++) {
            int dt = fixed_dt.get(i);
            if (dt == 1) {
                leng_v = 4;
            } else if (dt == 2) {
                leng_v = 1;
            } else if (dt == 3) {
                leng_v = 4;
            } else {
                leng_v = 8;
            }
            byte[] rec_bytes = new byte[leng_v];
            System.arraycopy(blockData, record_off + offset, rec_bytes, 0, leng_v);
            record[i] = convertBytesToObject(rec_bytes, dt);
            offset += leng_v;
        }
        int v_off=0;
        int data_off;
        int v_len;
        for(int i=0;i<var_dt_count;i++){
            data_off=((blockData[record_off+v_off+1]& 0xFF) << 8) | ((blockData[record_off+v_off] & 0xFF) );
            v_len = ((blockData[record_off+v_off+3] & 0xFF) << 8) | ((blockData[record_off+v_off+2] & 0xFF) );

//            byte[] array_to_be_printed = new byte[12] ;
//            System.arraycopy(blockData, record_off , array_to_be_printed, 0, 12);
//            System.out.println("Printing array");
//            for(int j=0;j<12;j++){
//                System.out.print(array_to_be_printed[j]+" ");
//            }

//            System.out.println((blockData[record_off+v_off+3]& 0xFF));
//            System.out.println((blockData[record_off+v_off+2] & 0xFF));
//            System.out.println(v_len);
            byte[] rec_bytes = new byte[v_len];
            System.arraycopy(blockData, record_off + data_off, rec_bytes, 0, v_len);
            record[fixed_dt_count+i] = convertBytesToObject(rec_bytes, 0);
            v_off+=4;
        }

        return record;
    }

    public Object converBytesToObject_seedha(byte[] columnDataBytes,int dt ){
        byte[] reverse_byte_array = new byte[columnDataBytes.length];
        for (int i = 0; i < columnDataBytes.length; i++) {
            reverse_byte_array[i] = columnDataBytes[columnDataBytes.length - i - 1];
        }
        return convertBytesToObject(reverse_byte_array,dt);
    }

    private Object convertBytesToObject(byte[] columnDataBytes,int dt) {
        switch (dt) {
            case 0:
                byte[] reverse_byte_array = new byte[columnDataBytes.length];
                for (int i = 0; i < columnDataBytes.length; i++) {
                    reverse_byte_array[i] = columnDataBytes[columnDataBytes.length - i - 1];
                }
                StringBuilder sb = new StringBuilder();
                for (byte b : reverse_byte_array) {
                    if (b == 0) {
                        break;
                    }
                    sb.append((char) b);
                }
                return sb.toString();
            case 1:
                return ((columnDataBytes[3] & 0xFF) << 24) |
                        ((columnDataBytes[2] & 0xFF) << 16) |
                        ((columnDataBytes[1] & 0xFF) << 8) |
                        (columnDataBytes[0] & 0xFF);
            case 2:
                return columnDataBytes[0] != 0;
            case 3:
                int intBits = ((columnDataBytes[3] & 0xFF) << 24) |
                        ((columnDataBytes[2] & 0xFF) << 16) |
                        ((columnDataBytes[1] & 0xFF) << 8) |
                        (columnDataBytes[0] & 0xFF);
                return Float.intBitsToFloat(intBits);
            case 4:
                long longBits = ((long)(columnDataBytes[7] & 0xFF) << 56) |
                        ((long)(columnDataBytes[6] & 0xFF) << 48) |
                        ((long)(columnDataBytes[5] & 0xFF) << 40) |
                        ((long)(columnDataBytes[4] & 0xFF) << 32) |
                        ((long)(columnDataBytes[3] & 0xFF) << 24) |
                        ((long)(columnDataBytes[2] & 0xFF) << 16) |
                        ((long)(columnDataBytes[1] & 0xFF) << 8) |
                        (columnDataBytes[0] & 0xFF);
                return Double.longBitsToDouble(longBits);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + ColumnType.values()[dt]);
        }
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        if (!check_file_exists(table_name) || check_index_exists(table_name, column_name)) {
            return false;
        }
        byte[] schema = get_data_block(table_name, 0);
        int column_number = colnum_from_colname(schema,column_name);
        int off_kaam = 2+ 2*column_number;
        if(column_number<0) return false;
        int hehe = ((schema[off_kaam+1] & 0xFF) << 8) | (schema[off_kaam] & 0xFF);
        int dt=(schema[hehe] & 0xFF);
        // Fetch records to determine column type
        List<Object> columnValues = new ArrayList<>();
        List<Integer> BlockValues = new ArrayList<>();

        for (int i = 1;; i++) {
            byte[] blockData = get_data_block(table_name, i);
            if (blockData == null) {
                break;
            }
//            System.out.println("Check int");
            List<Object[]> records = get_records_from_block(table_name, i);
//            System.out.println("Check int 2") ;
            for (Object[] record : records) {
                columnValues.add(record[column_number]);
                BlockValues.add(i);
            }
//            System.out.println("Check out");
        }
//        System.out.println("Check 3");
        // Create a B+ tree index
        if(dt==0){
            create_index0(table_name,column_name,order,columnValues,BlockValues);
        }else if(dt==1){
            create_index1(table_name,column_name,order,columnValues,BlockValues);
//            System.out.println("Check 4");
        }else if(dt==2){
            create_index2(table_name,column_name,order,columnValues,BlockValues);
        }else if(dt==3){
            create_index3(table_name,column_name,order,columnValues,BlockValues);
        }else if(dt==4){
            create_index4(table_name,column_name,order,columnValues,BlockValues);
        }
        return true;
    }

    void create_index0(String table_name,String column_name,int order, List<Object> columnValues, List<Integer> BlockValues){
        BPlusTreeIndexFile<String> b_index = new BPlusTreeIndexFile<>(order, String.class);
        for(int i=0;i< columnValues.size();i++){
            String value=(String)columnValues.get(i);
            b_index.insert(value,BlockValues.get(i));
        }
        int bPTIndex = db.addFile(b_index);
        file_to_fileid.put(table_name + "_" + column_name + "_index", bPTIndex);
    }
    void create_index1(String table_name,String column_name,int order, List<Object> columnValues, List<Integer> BlockValues){
        BPlusTreeIndexFile<Integer> b_index = new BPlusTreeIndexFile<>(order, Integer.class);
        for(int i=0;i< columnValues.size();i++){
            Integer value=(Integer)columnValues.get(i);
            b_index.insert(value,BlockValues.get(i));
        }
        int bPTIndex = db.addFile(b_index);
        file_to_fileid.put(table_name + "_" + column_name + "_index", bPTIndex);
    }
    void create_index2(String table_name,String column_name,int order, List<Object> columnValues, List<Integer> BlockValues){
        BPlusTreeIndexFile<Boolean> b_index = new BPlusTreeIndexFile<>(order, Boolean.class);
        for(int i=0;i< columnValues.size();i++){
            Boolean value=(Boolean) columnValues.get(i);
            b_index.insert(value,BlockValues.get(i));
        }
        int bPTIndex = db.addFile(b_index);
        file_to_fileid.put(table_name + "_" + column_name + "_index", bPTIndex);
    }
    void create_index3(String table_name,String column_name,int order, List<Object> columnValues, List<Integer> BlockValues){
        BPlusTreeIndexFile<Float> b_index = new BPlusTreeIndexFile<>(order, Float.class);
        for(int i=0;i< columnValues.size();i++){
            Float value=(Float) columnValues.get(i);
            b_index.insert(value,BlockValues.get(i));
        }
        int bPTIndex = db.addFile(b_index);
        file_to_fileid.put(table_name + "_" + column_name + "_index", bPTIndex);
    }
    void create_index4(String table_name,String column_name,int order, List<Object> columnValues, List<Integer> BlockValues){
        BPlusTreeIndexFile<Double> b_index = new BPlusTreeIndexFile<>(order, Double.class);
        for(int i=0;i< columnValues.size();i++){
            Double value=(Double)columnValues.get(i);
            b_index.insert(value,BlockValues.get(i));
        }
        int bPTIndex = db.addFile(b_index);
        file_to_fileid.put(table_name + "_" + column_name + "_index", bPTIndex);
    }
    int colnum_from_colname(byte[] schema_v, String column_name){
        int num_col = ((schema_v[1] & 0xFF) << 8) | (schema_v[0] & 0xFF);
        int offset_schema=2;
        int curr_off_v;
        for(int i=0;i<num_col;i++){
            curr_off_v=((schema_v[offset_schema+1] & 0xFF) << 8) | (schema_v[offset_schema] & 0xFF);
            int len_colname=(schema_v[curr_off_v+1] & 0xFF);
//            System.out.println(len_colname);
            byte[] colnamebytes = new byte[len_colname];
            offset_schema+=2;
            System.arraycopy(schema_v, curr_off_v+2, colnamebytes, 0, len_colname);
            String colname = (String) converBytesToObject_seedha(colnamebytes,0);
//            System.out.print(colname+"\n");
            if(column_name.equals(colname)){
                return i;
            }
        }
        return -1;


    }
    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        String index_file_name = table_name + "_" + column_name + "_index";
        return db.search_index(file_to_fileid.get(index_file_name),value);
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }
    public int file_to_file_id(String table_name,String column_name){
        return file_to_fileid.get(table_name + "_" + column_name + "_index");
    }
    public Object col_name_to_value(String column_name, Object[] record, String table_name){
        byte[] schema_v = get_data_block(table_name, 0);
        int col_num = colnum_from_colname(schema_v,column_name);
        return record[col_num];
    }

}