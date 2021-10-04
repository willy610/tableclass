use super::ex_distinct::ex_distinct;
use super::ex_groupby::ex_groupby;
use super::ex_join::ex_join;
use super::ex_join_test::ex_join_test;
use super::ex_join_multi::ex_join_multi;
use super::ex_groupby_own_function::ex_groupby_own_function;
use super::ex_where_eq_kind_depsubq::ex_where_eq_kind_depsubq;
use super::ex_where_eq_kind_once::ex_where_eq_kind_once;
use super::ex_where_exist_kind_depsub::ex_where_exist_kind_depsub;
use super::ex_limit::ex_limit;
use super::ex_orderby::ex_orderby;
use super::ex_project::ex_project;
use super::ex_project_sub::ex_project_sub;
use super::ex_query::ex_query;
use super::ex_where::ex_where;
use super::ex_join_and_project::ex_join_and_project;
use super::ex_where_set_kind_once::ex_where_set_kind_once;
use super::ex_with_recursive::ex_with_recursive;
use super::ex_set::ex_set;
use super::ex_scatter::ex_scatter;
use super::ex_explain::ex_explain;
/*

*/

pub fn chrisdate(dirpath: &str, verify_name: &str) {
    match verify_name {
        "ex_distinct" => ex_distinct(dirpath),
        "ex_groupby" => ex_groupby(dirpath),
        "ex_join" => ex_join(dirpath),
        "ex_join_test" => ex_join_test(dirpath),
        "ex_join_multi" => ex_join_multi(dirpath),
        "ex_groupby_own_function" => ex_groupby_own_function(dirpath),
        "ex_where_eq_kind_depsubq" => ex_where_eq_kind_depsubq(dirpath),
        "ex_where_eq_kind_once" => ex_where_eq_kind_once(dirpath),
        "ex_where_exist_kind_depsub" => ex_where_exist_kind_depsub(dirpath),
        "ex_limit" => ex_limit(dirpath),
        "ex_orderby" => ex_orderby(dirpath),
        "ex_project" => ex_project(dirpath),
        "ex_project_sub" => ex_project_sub(dirpath),
        "ex_query" => ex_query(dirpath),
        "ex_where" => ex_where(dirpath),
        "ex_join_and_project" => ex_join_and_project(dirpath),
        "ex_where_set_kind_once" => ex_where_set_kind_once(dirpath),
        "ex_with_recursive" => ex_with_recursive(dirpath),
        "ex_set" => ex_set(),
        "ex_scatter" => ex_scatter(dirpath),
        "ex_explain" => ex_explain(dirpath),

        /*
        
*/
        _ => {
            panic!("Unknown verify_name={}", verify_name)
        }
    }
}
