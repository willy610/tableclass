/*
use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_limit::Params2Limit;
use tableclass::table_folder::table_orderby::Params2OrderBy;
use tableclass::table_folder::table_select::Params2Select;

use std::cmp::Ordering;
*/
use super::popingredients::popingredients;
use super::complexrecepies::complexrecepies;

pub fn recepies(dirpath: &str, verify_name: &str) {
    match verify_name {
        "popingredients" => popingredients(dirpath),
        "complexrecepies" => complexrecepies(dirpath),
        _ => {
            panic!("Unknown verify_name={}", verify_name)
        }
    }
    return;
/*
    let recepie = Table::from_csv(Params2FromCSV {
        table_name: "recepie",
        dir: dir,
        file: "recepies.csv",
        filter: None,
        project: None,
    });

    let recepiestep = Table::from_csv(Params2FromCSV {
        table_name: "recepiestep",
        dir: dir,
        file: "recepiestep.csv",
        filter: None,
        project: None,
    });

    let ingredient = Table::from_csv(Params2FromCSV {
        table_name: "ingredient",
        dir: dir,
        file: "ingredient.csv",
        filter: None,
        project: None,
    });
    let recepiesstepingredient = Table::from_csv(Params2FromCSV {
        table_name: "recepiesstepingredient",
        dir: dir,
        file: "recepiesstepingredient.csv",
        filter: None,
        project: None,
    });

    match verify_name {
        "verify_2" => {
            let _the_sql = "
(most used ingredient)
SELECT groupedon.recid,
       recepies.recname,
       groupedon.count
FROM
  (SELECT recid,
          Count(*) AS COUNT
   FROM recepistep GROUP  BY recid) AS groupedon,
     recepies
WHERE recepies.recid = groupedon.recid
  ORDER  BY groupedon.count DESC
  LIMIT 0, 20";

            let thejoin = recepiesstepingredient.table_join(
                Some(
                    "ON recepiesstepingredient.ingredientid = ingredient.ingredientid".to_string(),
                ),
                Params2Join {
                    right_table: &ingredient,
                    joinkindandcols: None,
                    cond_left: None,
                    cond_right: None,
                    cond_both: Some(
                        |left: &Table,
                         right: &Table,
                         leftrow: &TableRow,
                         rightrow: &TableRow|
                         -> bool {
                            let colindex_left = left.colname_2_index("ingredientid");
                            let colindex_right = right.colname_2_index("ingredientid");
                            leftrow[colindex_left] == rightrow[colindex_right]
                        },
                    ),
                },
            );
            let result = thejoin.table_groupby(
                Some(
                    "SELECT ingredient.ingredientname,COUNT(*) GROUP BY ingredient.ingredientname"
                        .to_string(),
                ),
                Params2GroupBy {
                    out_table: "result",
                    groupon: Some(vec!["ingredient.ingredientname"]),
                    aggrcols: vec![vec!["count", "*", "Used in number of recepies"]],
                    custom_aggr: None,
                    cond_having: None,
                },
            );
            let result = result.table_orderby(
                Some("ORDER BY 2 DESC".to_string()),
                Params2OrderBy {
                    order_cols: None,
                    ordering_callback: Some(
                        |obj: &Table, a: &TableRow, b: &TableRow| -> Ordering {
                            //                        let colindex_recepiestep_aggr_count = 1;
                            let colindex_recepiestep_aggr_count =
                                obj.colname_2_index("Used in number of recepies").unwrap();

                            let left: i32 = a[colindex_recepiestep_aggr_count].parse().unwrap();
                            let right: i32 = b[colindex_recepiestep_aggr_count].parse().unwrap();
                            // order by descending
                            if left < right {
                                Ordering::Greater
                            } else if left > right {
                                Ordering::Less
                            } else {
                                Ordering::Equal
                            }
                        },
                    ),
                },
            );
            let result = result.table_limit(
                Some("LIMIT 0,20".to_string()),
                Params2Limit {
                    offset: None,
                    max_number_rows: Some(20),
                },
            );

            eprintln!("result=\n{}", result.show());
            explain(&result, _the_sql);
        }
        "verify_1" => {
            let _the_sql = "
(most used ingredient)
SELECT groupedon.recid,
       recepies.recname,
       groupedon.count
FROM
  (SELECT recid,
          Count(*) AS COUNT
   FROM recepistep GROUP  BY recid) AS groupedon,
     recepies
WHERE recepies.recid = groupedon.recid
  ORDER  BY groupedon.count DESC
LIMIT 0, 20";

            let result = recepiestep
                .table_groupby(
                    Some("SELCET recid,COUNT(*) GROUP BY recid".to_string()),
                    Params2GroupBy {
                        out_table: "groupedon",
                        groupon: Some(vec!["recid"]),
                        aggrcols: vec![vec!["count", "*", "COUNT(*)"]],
                        custom_aggr: None,
                        cond_having: None,
                    },
                )
                .table_join(
                    Some("ON groupedon.recid = recepie.recid ".to_string()),
                    Params2Join {
                        right_table: &recepie,
                        joinkindandcols: None,
                        cond_left: None,
                        cond_right: None,
                        cond_both: Some(
                            |left: &Table,
                             right: &Table,
                             leftrow: &TableRow,
                             rightrow: &TableRow|
                             -> bool {
                                let colindex_left = left.colname_2_index("recid");
                                let colindex_right = right.colname_2_index("recid");
                                leftrow[colindex_left] == rightrow[colindex_right]
                            },
                        ),
                    },
                )
                .table_select(
                    Some("SELECT groupedon.recid, recepie.recname, groupedon.COUNT(*)".to_string()),
                    Params2Select {
                        from_cols: vec!["groupedon.recid", "recepie.recname", "groupedon.COUNT(*)"],
                        to_table_name: None,
                        to_column_names: Some(vec![
                            "Recepiid",
                            "Recepinamee",
                            "Number of ingredients in recepie",
                        ]),
                    },
                )
                .table_orderby(
                    Some("ORDER BY count desc".to_string()),
                    Params2OrderBy {
                        order_cols: None,
                        ordering_callback: Some(
                            |obj: &Table, a: &TableRow, b: &TableRow| -> Ordering {
                                //                            let colindex_recepiestep_aggr_count = 2;
                                let colindex_recepiestep_aggr_count =
                                    obj.colname_2_index("Number of ingredients in recepie");

                                let left: i32 = a[colindex_recepiestep_aggr_count].parse().unwrap();
                                let right: i32 =
                                    b[colindex_recepiestep_aggr_count].parse().unwrap();
                                // order by descending
                                if left < right {
                                    Ordering::Greater
                                } else if left > right {
                                    Ordering::Less
                                } else {
                                    Ordering::Equal
                                }
                            },
                        ),
                    },
                )
                .table_limit(
                    Some("LIMIT 0,20".to_string()),
                    Params2Limit {
                        offset: None,
                        max_number_rows: Some(20),
                    },
                );
            eprintln!("{}\n{}", _the_sql, result.show());
            explain(&result, _the_sql);
        }
        _ => {
            panic!("Unknown verify case number")
        }
    }
    */
}
