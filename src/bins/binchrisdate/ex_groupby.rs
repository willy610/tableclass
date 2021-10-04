use tableclass::explain::explain;
use tableclass::table::Table;
use tableclass::table_folder::table_from::Params2FromCSV;

use tableclass::table_folder::table_groupby::Params2GroupBy;

pub fn ex_groupby(dirpath: &str) {
    /*--------------------------------------------------------*/
    // GROUP BY operations
    //
    // One can do aggregation on groups or without groups
    // For each row there is a callback with an aggragation 'row' (sofar)
    // The following is an example of traditional functions but anything could be implemented

    /*--------------------------------------------------------*/
    let the_sql = "select CITY,MAX(WEIGHT),MIN(WEIGHT),COUNT(*),SUM(WEIGHT),AVG(WEIGHT)
    from P 
    group by city";
    let outer = || -> Result<Table, String> {
        let _outline = Table::table_from_dummy()
            .table_from_outline("FROM CSV parts", "Params")
            .table_groupby_outline(
                "GROUP BY CITY aggr on MAX(WEIGHT),MIN(WEIGHT),COUNT(*),SUM(WEIGHT),AVG(WEIGHT)",
                "Params",
            )
            .table_select_outline(
                "CITY,max(WEIGHT),min(WEIGHT),count(WEIGHT),sum(WEIGHT),avg(WEIGHT)",
                "Params",
            );
        let result = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?
        .table_groupby(
            Some("SELECT MAX(WEIGHT),MIN(WEIGHT),COUNT(*),SUM(WEIGHT),AVG(WEIGHT)".to_string()),
            Params2GroupBy {
                out_table: "test_aggr_no_group",
                groupon: Some(vec!["CITY"]),
                aggrcols: vec![
                    vec!["max", "WEIGHT", "MAX(WEIGHT)"],
                    vec!["min", "WEIGHT", "MIN(WEIGHT)"],
                    vec!["count", "*", "COUNT(*)"],
                    vec!["sum", "WEIGHT", "SUM(WEIGHT)"],
                    vec!["avg", "WEIGHT", "AVG(WEIGHT)"],
                ],
                custom_aggr: None,
                cond_having: None,
            },
        );
        // Show intermediate result
        eprintln!("{}",(result.clone().unwrap()).show());
        result
    };
    match outer(){
        Ok(result_set)=>{
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational 
        }
        Err(err)=>{eprintln!("Some error in 'ex_distinct' : {}", err);}

    }
}
