#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>

#include <list>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilterBase *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    const auto & storage_row_level_filter = source_step_with_filter->getRowLevelFilter();
    if (storage_row_level_filter)
        source_step_with_filter->addFilter(storage_row_level_filter->actions.clone(), storage_row_level_filter->column_name);
    if (storage_prewhere_info)
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);

    /// Collect ExpressionStep DAGs encountered while walking up the plan.
    /// When a filter references columns produced by expressions (e.g., ALIAS
    /// columns computed in "Compute alias columns" step, or renamed in
    /// "Change column names to column identifiers" step), we compose the
    /// filter through these expression DAGs so that column references are
    /// resolved to physical columns. This is essential for correct index
    /// analysis when plan optimizations like mergeExpressions have not
    /// merged these steps into the filter.
    std::vector<const ActionsDAG *> expression_dags;
    /// Owns the DAGs synthesized for ARRAY JOIN steps below; `expression_dags` points into it.
    std::list<ActionsDAG> array_join_dags;
    bool passed_array_join = false;

    /// Compose a filter through the accumulated DAGs (in bottom-to-top order), so its column
    /// references resolve to the physical columns of the source.
    auto compose = [&](ActionsDAG filter_dag)
    {
        for (auto it = expression_dags.rbegin(); it != expression_dags.rend(); ++it)
            filter_dag = ActionsDAG::merge((*it)->clone(), std::move(filter_dag));
        return filter_dag;
    };

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            /// This resolves column identifiers to their underlying expressions, enabling correct
            /// index matching for ALIAS columns and renamed columns.
            source_step_with_filter->addFilter(compose(filter_step->getExpression().clone()), filter_step->getFilterColumnName());
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            /// An ARRAY JOIN changes the row count, so a LIMIT above it says nothing about the source.
            if (!passed_array_join)
                source_step_with_filter->setLimit(limit_step->getLimitForSorting());
            break;
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            /// `arrayJoin` in an `ExpressionStep` above the source changes row cardinality.
            /// Propagating the outer `LIMIT` past such a step is unsound: the source would
            /// be told to generate at most N rows, and `arrayJoin` would then expand only
            /// those (possibly producing fewer than N output rows when arrays are empty,
            /// or wrong rows when arrays expand). Composing filters through `arrayJoin`
            /// expressions is unsound for the same reason. Stop walking here and skip both
            /// filter composition and limit propagation. See issue #82279 and the sibling
            /// guards in `liftUpFunctions`, `optimizeLazyMaterialization`, `optimizeTopK`,
            /// `topKThroughJoin`, and `pushLimitByIntoSort`.
            if (expression_step->getExpression().hasArrayJoin())
                break;
            expression_dags.push_back(&expression_step->getExpression());
            continue;
        }
        else if (auto * array_join_step = typeid_cast<ArrayJoinStep *>(iter->node->step.get()))
        {
            /// Every output row of a plain ARRAY JOIN comes from a real element, so a condition on the element
            /// holds for some element of the array: rewriting the element back to `arrayJoin(col)` gives the
            /// indexes the shape they already read as `has(col, x)`. LEFT and unaligned joins also emit default
            /// values for empty or short arrays, and such a row says nothing about the array.
            if (array_join_step->isLeft() || array_join_step->isUnaligned())
                break;

            auto & dag = array_join_dags.emplace_back(array_join_step->getInputHeaders().front()->getNamesAndTypesList());
            for (const auto & name : array_join_step->getColumns())
            {
                const auto & array = dag.findInOutputs(name);
                /// The alias keeps the step's output name for the composition; the `arrayJoin(col)` node underneath
                /// is what the index analysis names the atom by, so it can't be mistaken for the array column itself.
                const auto & element = dag.addAlias(dag.addArrayJoin(array, {}), name);
                for (auto & output : dag.getOutputs())
                    if (output == &array)
                        output = &element;
            }
            expression_dags.push_back(&dag);
            passed_array_join = true;

            /// A fused element filter runs inside the step; for the index it is a filter right above the join.
            if (const auto * element_filter = array_join_step->getElementFilter())
                source_step_with_filter->addFilter(compose(element_filter->clone()), array_join_step->getElementFilterColumnName());
        }
        else if (auto * object_filter_step = typeid_cast<ObjectFilterStep *>(iter->node->step.get()))
        {
            /// Not composed through the DAGs below, so above an ARRAY JOIN its column names would mean the elements.
            if (passed_array_join)
                break;
            source_step_with_filter->addFilter(object_filter_step->getExpression().clone(), object_filter_step->getFilterColumnName());
        }
        else
        {
            break;
        }
    }

    source_step_with_filter->applyFilters();
}

}
