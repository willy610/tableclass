use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_limit::Params2Limit;
use tableclass::table_folder::table_orderby::Params2OrderBy;
use tableclass::table_folder::table_select::Params2Select;

use std::cmp::Ordering;

pub fn complexrecepies(dirpath: &str) {
    let the_sql = "(recepies with most ingredients)
SELECT groupedon.recid,
    recepies.recname,
    groupedon.count
FROM
  (SELECT recid,
          Count(*) AS COUNT
   FROM recepistep GROUP  BY recid)
   AS groupedon,
   recepies
WHERE recepies.recid = groupedon.recid
  ORDER  BY groupedon.count DESC
  LIMIT 0, 20";
    let outer = || -> Result<Table, String> {
        let recepie = Table::from_csv(Params2FromCSV {
            table_name: "recepie",
            dir: dirpath,
            file: "recepies.csv",
            filter: None,
            project: None,
        });
        let recepiestep = Table::from_csv(Params2FromCSV {
            table_name: "recepiestep",
            dir: dirpath,
            file: "recepiestep.csv",
            filter: None,
            project: None,
        });
        let result = recepiestep?
            .table_groupby(
                Some("SELCET recid,COUNT(*) GROUP BY recid".to_string()),
                Params2GroupBy {
                    out_table: "groupedon",
                    groupon: Some(vec!["recid"]),
                    aggrcols: vec![vec!["count", "*", "COUNT(*)"]],
                    custom_aggr: None,
                    cond_having: None,
                },
            )?
            .table_join(
                Some("ON groupedon.recid = recepie.recid ".to_string()),
                Params2Join {
                    right_table: &recepie?,
                    joinkindandcols: None,
                    cond_left: None,
                    cond_right: None,
                    cond_both: Some(
                        |left: &Table,
                         right: &Table,
                         leftrow: &TableRow,
                         rightrow: &TableRow|
                         -> Result<bool, String> {
                            let colindex_left = left.colname_2_index("recid")?;
                            let colindex_right = right.colname_2_index("recid")?;
                            Ok(leftrow[colindex_left] == rightrow[colindex_right])
                        },
                    ),
                    project: None,
                },
            )?
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
            )?
            .table_orderby(
                Some("ORDER BY count desc".to_string()),
                Params2OrderBy {
                    order_cols: None,
                    ordering_callback: Some(
                        |obj: &Table, a: &TableRow, b: &TableRow| -> Ordering {
                            //                            let colindex_recepiestep_aggr_count = 2;
                            let colindex_recepiestep_aggr_count = obj
                                .colname_2_index("Number of ingredients in recepie")
                                .unwrap();

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
            )?
            .table_limit(
                Some("LIMIT 0,20".to_string()),
                Params2Limit {
                    offset: None,
                    max_number_rows: Some(20),
                },
            );

        result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'complexrecepies' : {}", err);
        }
    }
}
