package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

/*
    * PProjectFilter is a relational operator that represents a Project followed by a Filter.
    * You need to write the entire code in this file.
    * To implement PProjectFilter, you can extend either Project or Filter class.
    * Define the constructor accordinly and override the methods as required.
*/
public class PProjectFilter extends Project implements PRel {
    private final RexNode condition;
    private Object[] next_row;  
    int next_row_length ;
    private ArrayList<RexNode> projects_copy;
    
    public PProjectFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RexNode cc ,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.condition = cc;
        this.next_row_length=projects.size();
        projects_copy = new ArrayList<RexNode>();
        for(int i=0;i<projects.size();i++){
            projects_copy.add(projects.get(i));
        }
    }

    public PProjectFilter copy(RelTraitSet traitSet, RelNode input,
                         List<RexNode> projects  ,RelDataType rowType) {
        return new PProjectFilter(getCluster(), traitSet, input, projects , condition ,rowType);
    }
    public String toString() {
        return "PProjectFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        if(((PRel) input).open()){
            return true;
        }
        /* Write your code here */
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        PRel inp = (PRel) input;
        inp.close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        if(next_row != null) return true;
        PRel inp = (PRel) input;
        while(inp.hasNext()){
            Object[] row = inp.next();
            if(row != null && check_row(row, input.getRowType(),condition)){
                next_row = row;
                return true;
            }
        }
        return false;
    }
    private boolean check_row(Object[] row, RelDataType rowType, RexNode cond){
        if(cond instanceof RexCall){
            RexCall call = (RexCall) cond;
            SqlKind kind = call.getKind();
            if(kind == SqlKind.AND){
                return check_and(call, row, rowType);
            }else if(kind==SqlKind.OR){
                return check_or(call, row, rowType);
            }else{
                return eval(call, row, rowType);
            }
        }
        return false;
    }
    private boolean check_and(RexCall call, Object[] row, RelDataType rowType){
        for(RexNode operand: call.operands){
            if(!check_row(row, rowType,operand)){
                return false;
            }
        }
        return true;
    }
    private boolean check_or(RexCall call, Object[] row, RelDataType rowType){
        for(RexNode operand: call.operands){
            if(check_row(row, rowType,operand)){
                return true;
            }
        }
        return false;
    }
    private boolean eval(RexCall call, Object[] row, RelDataType rowType){
        Object left = call.operands.get(0);
        Object right = call.operands.get(1);
        Object left_val = eval_rex(left, row, rowType);
        Object right_val = eval_rex(right, row, rowType);
        if(left_val==null || right_val==null){
            return false;
        }
        if(left_val instanceof Number && right_val instanceof Number){
            Double left_d = ((Number)left_val).doubleValue();
            Double right_d = ((Number)right_val).doubleValue();
            int ans = left_d.compareTo(right_d);
            return final_check(call, ans);
        }else if(left_val instanceof String && right_val instanceof String){
            int ans=left_val.toString().compareTo(right_val.toString());
            return final_check(call, ans);
        }
        logger.error("uncomparable types");
        return false;
    }
    private Object eval_rex(Object rex, Object[] row, RelDataType rowType){
        if(rex instanceof RexLiteral){
            return ((RexLiteral)rex).getValue3();
        }
        if(rex instanceof RexInputRef){
            return row[((RexInputRef)rex).getIndex()];
        }
        if(rex instanceof RexCall){
            
            Object obj1 = eval_rex(((RexCall)rex).getOperands().get(0), row, rowType);
            Object obj2 = eval_rex(((RexCall)rex).getOperands().get(1), row, rowType);
            
            if(!(obj1 instanceof Number) || !(obj2 instanceof Number)){
                logger.error("Wrong Type");
                return null;
            }
            Double n1 = ((Number)obj1).doubleValue();
            Double n2 = ((Number)obj2).doubleValue();

            switch (((RexCall)rex).getKind()) {
                case PLUS:
                    return n1+n2;
                case MINUS:
                    return n1-n2;
                case TIMES:
                    return n1*n2;
                case DIVIDE:
                    if(n2 == 0){
                        logger.error("Division by Zero");
                        return null;
                    }
                    return n1/n2;
                default :
                    logger.error("Unsupported arithmetic operation: ");
                    return null;
            }
        }
        logger.error("Unsupported RexNode Type");
        return null;

    }

    private boolean final_check(RexCall call,int ans){
        SqlKind kind=call.getOperator().getKind();
        switch (kind) {
            case EQUALS:
                return ans==0;
            case NOT_EQUALS:
                return ans!=0;
            case LESS_THAN:
                return ans<0;
            case LESS_THAN_OR_EQUAL:
                return ans<=0;
            case GREATER_THAN:
                return ans>0;
            case GREATER_THAN_OR_EQUAL:
                return ans>=0;
            default:
                logger.error("Unsupported comparison: ");
                return false;
        }
    }
    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        if(this.hasNext()){
            Object[] row = project(next_row);
            next_row = null;
            return row;
        }
        return null;
    
    }
    private Object[] project(Object[] row){
        Object[] next_r = new Object[next_row_length];
        if(input == null){
            return null;
        }
        for(int i=0;i<next_row_length;i++){
            RexInputRef ref = (RexInputRef) (projects_copy.get(i));
            next_row[i]=row[ref.getIndex()];
        }
        return next_r;
    }
}
