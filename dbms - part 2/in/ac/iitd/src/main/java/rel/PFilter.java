package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import convention.PConvention;

import java.math.BigDecimal;
import java.util.List;

public class PFilter extends Filter implements PRel {
    private Object[] next_row;  


    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
        this.next_row= null;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        PRel inp = (PRel) input;
        if(!inp.open()) {
            logger.error("Error in opening PFilter");
            return false;
        }
        
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        PRel inp = (PRel) input;
        inp.close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
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
    private boolean check_row(Object[] row, RelDataType rowType, RexNode condition){
        if(condition instanceof RexCall){
            RexCall call = (RexCall) condition;
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
            BigDecimal left_d = new BigDecimal(left_val.toString());
            BigDecimal right_d = new BigDecimal(right_val.toString());
            int ans = left_d.compareTo(right_d);
            return final_check(call.getOperator().getKind(), ans);
        }else if(left_val instanceof String && right_val instanceof String){
            return final_check(call.getOperator().getKind(), left_val.toString().compareTo(right_val.toString()));
        }else{
            logger.error("Cannot Compare");
            return false;
        }
    }
    private Object eval_rex(Object rex, Object[] row, RelDataType rowType){
        if(rex instanceof RexCall){
            List<RexNode> operands = ((RexCall)rex).getOperands();
            Object obj1 = eval_rex(operands.get(0), row, rowType);
            Object obj2 = eval_rex(operands.get(1), row, rowType);
            SqlKind kind = ((RexCall)rex).getKind();
            if(!(obj1 instanceof Number) || !(obj2 instanceof Number)){
                logger.error("Numeric Types Required");
                return null;
            }
            switch (kind) {
                case PLUS:
                    return ((Number)obj1).doubleValue() + ((Number)obj2).doubleValue();
                case MINUS:
                    return ((Number)obj1).doubleValue() - ((Number)obj2).doubleValue();
                case TIMES:
                    return ((Number)obj1).doubleValue() * ((Number)obj2).doubleValue();
                case DIVIDE:
                    if(((Number)obj2).doubleValue() == 0){
                        logger.error("Division by Zero");
                        return null;
                    }
                    return ((Number)obj1).doubleValue() / ((Number)obj2).doubleValue();
                default :
                    logger.error("Unsupported arithmetic operation: ");
                    return null;
            }
        }
        if(rex instanceof RexInputRef){
            return row[((RexInputRef)rex).getIndex()];
        }
        if(rex instanceof RexLiteral){
            return ((RexLiteral)rex).getValue3();
        }
        logger.error("unknown RexNode Type");
        return null;

    }
    private Boolean final_check(SqlKind kind, int ans){
        switch (kind) {
            case GREATER_THAN:
                return ans>0;
            case GREATER_THAN_OR_EQUAL:
                return ans>=0;
            case LESS_THAN:
                return ans<0;
            case LESS_THAN_OR_EQUAL:
                return ans<=0;
            case EQUALS:
                return ans==0;
            case NOT_EQUALS:
                return ans!=0;
            default:
                logger.error("Unsupported Comparison");
                return false;
        }
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        if(this.hasNext()){
            Object[] row = next_row;
            next_row = null;
            return row;
        }
        return null;
    }
    
}
