package rel;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Internal.IntList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {
    private List<Object[]> leftRows;
    private List<Integer> lkeys,rkeys;
    private HashMap<List<Object>,ArrayList<Object[]>> hashTable;
    private HashMap<Object[],Boolean> matched;
    private List<Object[]> joined_rows;
    private int left_row_size,right_row_size;
    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
                leftRows=new ArrayList<Object[]>();
                this.lkeys = joinInfo.leftKeys;
                this.rkeys = joinInfo.rightKeys;
                hashTable=new HashMap<List<Object>,ArrayList<Object[]>>();
                matched=new HashMap<Object[],Boolean>();
                joined_rows=new ArrayList<Object[]>();
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        PRel inpl=((PRel)getInput(0));
        PRel inpr=((PRel)getInput(1));
        if(!inpl.open() || !inpr.open()) return false;
        left_row_size=(getInput(0)).getRowType().getFieldCount();
        right_row_size=(getInput(1)).getRowType().getFieldCount();
        if(rkeys.size()!=lkeys.size()){
            logger.error("Join keys are not of same size");
            return false;
        }

        while (inpl.hasNext()) {
            Object[] lrow = inpl.next();
            leftRows.add(lrow);
        }
        hash_put();
        return true;
    }


    private void hash_put(){
        for(Object[] row:leftRows){
            matched.put(row, false);
            List<Object> hash=new ArrayList<Object>();
            for(int key:lkeys){
                hash.add(row[key]);
            }
            if(hashTable.containsKey(hash)){
                hashTable.get(hash).add(row);
            }
            else{
                ArrayList<Object[]> temp=new ArrayList<Object[]>();
                temp.add(row);
                hashTable.put(hash,temp);
            }
        }
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        leftRows.clear();
        hashTable.clear();
        joined_rows.clear();
        matched.clear();
        ((PRel)getInput(0)).close();
        ((PRel)getInput(1)).close();
        /* Write your code here */
        return;
    }
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        if(joined_rows!=null && joined_rows.size()>0) return true;
        PRel inpr=((PRel)this.getInput(1));
        if(inpr.hasNext()){
            Object[] rrow=inpr.next();
            List<Object> hash=new ArrayList<Object>();
            for(int key:rkeys){
                hash.add(rrow[key]);
            }
            if(hashTable.containsKey(hash)){
ArrayList<Object[]> temp=hashTable.get(hash);
                getJoinedRows(temp, rrow,hash);
                return true;
            }
            if(joinType==JoinRelType.RIGHT||joinType==JoinRelType.FULL){
                getrightJoinedRows(rrow);
            }
//            if(joined_rows!=null && joined_rows.size()>0) return true;
//            return false;
            return hasNext();
        }
        if(joinType==JoinRelType.LEFT||joinType==JoinRelType.FULL){
            getleftJoinedRows();
            if(joined_rows!=null && joined_rows.size()>0) return true;
            return false;
        }
        matched.clear();
        if(joined_rows!=null && joined_rows.size()>0) return true;
        return false;
    }
    private void getJoinedRows(List<Object[]> temp,Object[] row,List<Object> hash){
        for(Object[] lrow:temp){
            matched.put(lrow, true);
            Object[] joined_row=new Object[left_row_size+right_row_size];
            for(int i=0;i<left_row_size;i++){
                joined_row[i]=lrow[i];
            }
            for(int i=0;i<right_row_size;i++){
                joined_row[i+left_row_size]=row[i];
            }
            joined_rows.add(joined_row);
        }
    }
    private void getrightJoinedRows(Object[] row){
        Object[] joined_row=new Object[left_row_size+right_row_size];
        for(int i=0;i<left_row_size;i++){
            joined_row[i]=null;
        }
        for(int i=0;i<right_row_size;i++){
            joined_row[i+left_row_size]=row[i];
        }
        joined_rows.add(joined_row);
    }
    private void getleftJoinedRows(){
        for(Object[] lrow:matched.keySet()){
            if(matched.get(lrow)){
                continue;
            }
            matched.put(lrow,true);
            Object[] joined_row=new Object[left_row_size+right_row_size];
            for(int i=0;i<left_row_size;i++){
                joined_row[i]=lrow[i];
            }
            for(int i=0;i<right_row_size;i++){
                joined_row[i+left_row_size]=null;
            }
            joined_rows.add(joined_row);
        }
        matched.clear();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        if (joined_rows != null && joined_rows.size() > 0) {
            return joined_rows.remove(0);
        }
        return null;
    }
}
