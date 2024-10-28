package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;

import convention.PConvention;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {
    int next_row_length ;
    private ArrayList<RexNode> projects_copy;
    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
        this.next_row_length=projects.size();
        projects_copy = new ArrayList<RexNode>();
        for(int i=0;i<projects.size();i++){
            projects_copy.add(projects.get(i));
        }
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        if(((PRel) input).open()){
            return true;
        }
        /* Write your code here */
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        ((PRel)input).close();
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        if(((PRel)input).hasNext()){
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        if(!(input instanceof PRel)){
            return null;
        }
        Object[] row = ((PRel)input).next();
        Object[] next_row = new Object[next_row_length];
        if(input == null){
            return null;
        }
        for(int i=0;i<next_row_length;i++){
            RexInputRef ref = (RexInputRef) (projects_copy.get(i));
            next_row[i]=row[ref.getIndex()];
        }
        return next_row;
    }
}
