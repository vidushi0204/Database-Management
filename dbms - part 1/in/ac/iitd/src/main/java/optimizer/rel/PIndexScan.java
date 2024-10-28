package optimizer.rel;

import index.bplusTree.BPlusTreeIndexFile;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import manager.StorageManager;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import storage.DB;
import sun.util.resources.cldr.zh.CalendarData_zh_Hans_HK;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {

    private final List<RexNode> projects;
    private final RelDataType rowType;
    private final RelOptTable table;
    private final RexNode filter;

    public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
        super(cluster, traitSet, table);
        this.table = table;
        this.rowType = deriveRowType();
        this.filter = filter;
        this.projects = projects;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PIndexScan(getCluster(), traitSet, table, filter, projects);
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "PIndexScan";
    }

    public String getTableName() {
        return table.getQualifiedName().get(1);
    }
    public boolean greater_equal_cmp(Object key1, Object key2){
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

    private int hash_this(Object o){
        return o.hashCode() ;
    }
    private int hash_of_a_record(Object[] record){
        int hash_ans = 3123 ;
        int len = record.length ;
        for (int i = 0 ; i < len ; i++){
            if (record[i]==null){
                hash_ans*=i ;
                hash_ans%=5197716 ;
            }
            hash_ans = (hash_ans*i + hash_this(record[i]))%5197716 ;
        }
        return hash_ans;
    }
    @Override
    public List<Object[]> evaluate(StorageManager storage_manager) {
        String tableName = getTableName();
        System.out.println("Evaluating PIndexScan for table: " + tableName);

        RexCall rex_call = (RexCall) filter;
        SqlBinaryOperator operator = (SqlBinaryOperator) rex_call.getOperator();
        List<RexNode> operand_list = rex_call.getOperands();
        RexNode left_operand = operand_list.get(0);
        RexInputRef left_input_ref = (RexInputRef) left_operand;
        int column_ind = left_input_ref.getIndex();
        RexLiteral right_operand = (RexLiteral) operand_list.get(1);
        String column_name = table.getRowType().getFieldNames().get(column_ind);
        RelDataType column_type = table.getRowType().getFieldList().get(column_ind).getType();

        DB db = storage_manager.getDb();
        int file_id = storage_manager.file_to_file_id(tableName,column_name);
        BPlusTreeIndexFile<Object> b_index=db.get_bpt(file_id);
        if(b_index == null){
            System.out.println("Index not found");
            return null;
        }
        List<Object[]> ret_list = new ArrayList<Object[]>();
        List<Integer> blocks;
        Class c =b_index.give_type();

        Set<Integer> lmao_set_record = new HashSet<>() ;

        SqlKind kind = rex_call.getKind();
        if(kind.toString().equals("EQUALS")){
            blocks = b_index.equal_block_list(right_operand.getValueAs(c));
            for (int block : blocks) {
                List<Object[]> records = storage_manager.get_records_from_block(tableName, block);
                for (Object[] record : records) {
                    Object record_value  =  storage_manager.col_name_to_value(column_name, record,tableName );
                    if (record_value == null) {
                        continue;
                    }
                    if (lmao_set_record.contains(hash_of_a_record(record))){continue; }
                    lmao_set_record.add(hash_of_a_record(record)) ;
                    if(record_value.equals(right_operand.getValueAs(c))){
                        ret_list.add(record) ;
                    }
                }
            }
        }
        else if(kind.toString().equals("GREATER_THAN")){
            blocks = b_index.greater_block_list(right_operand.getValueAs(c));
            for (int block : blocks) {
                List<Object[]> records = storage_manager.get_records_from_block(tableName, block);
                for (Object[] record : records) {
                    Object record_value  =  storage_manager.col_name_to_value(column_name, record,tableName );
                    if (lmao_set_record.contains(hash_of_a_record(record))){continue; }
                    lmao_set_record.add(hash_of_a_record(record)) ;
                    if (greater_equal_cmp( record_value , right_operand.getValueAs(c)) & !right_operand.getValueAs(c).equals(record_value)) {
                        ret_list.add(record) ;
                    }
                }
            }
        }
        else if(kind.toString().equals("GREATER_THAN_OR_EQUAL")){
            blocks = b_index.geq_block_list(right_operand.getValueAs(c));
            for (int block : blocks) {
                List<Object[]> records = storage_manager.get_records_from_block(tableName, block);
                for (Object[] record : records) {
                    Object record_value  =  storage_manager.col_name_to_value(column_name, record,tableName );
                    if (lmao_set_record.contains(hash_of_a_record(record))){continue; }
                    lmao_set_record.add(hash_of_a_record(record)) ;
                    if (greater_equal_cmp( record_value , right_operand.getValueAs(c))) {
                        ret_list.add(record) ;
                    }
                }
            }
        }
        else if(kind.toString().equals("LESS_THAN")){
            blocks = b_index.less_block_list(right_operand.getValueAs(c));
            for (int block : blocks) {
                List<Object[]> records = storage_manager.get_records_from_block(tableName, block);
                for (Object[] record : records) {
                    Object record_value  =  storage_manager.col_name_to_value(column_name, record,tableName );
                    if (lmao_set_record.contains(hash_of_a_record(record))){continue; }
                    lmao_set_record.add(hash_of_a_record(record)) ;
                    if (!greater_equal_cmp( record_value , right_operand.getValueAs(c))) {
                        ret_list.add(record) ;
                    }
                }
            }
        }
        else if(kind.toString().equals("LESS_THAN_OR_EQUAL")){
            blocks = b_index.leq_block_list(right_operand.getValueAs(c));
            for (int block : blocks) {
                List<Object[]> records = storage_manager.get_records_from_block(tableName, block);
                for (Object[] record : records) {
                    Object record_value  =  storage_manager.col_name_to_value(column_name, record,tableName );
                    if (lmao_set_record.contains(hash_of_a_record(record))){continue; }
                    lmao_set_record.add(hash_of_a_record(record)) ;
                    if (greater_equal_cmp(right_operand.getValueAs(c) , record_value)) {
                        ret_list.add(record) ;
                    }
                }
            }
        }
        else{
            System.out.println("Invalid operator");
            return null;
        }

        /* Write your code here */

        return ret_list;
    }
}