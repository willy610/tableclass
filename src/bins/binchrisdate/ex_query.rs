use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_orderby::Params2OrderBy;
pub fn ex_query(dirpath: &str) {
    let the_sql = "SELECT p.pno,
           p.weight,
           p.color,
           Max(sp.qty),
           Sum(sp.qty)
    FROM   p,
           sp
    WHERE  p.pno = sp.pno
           AND ( p.color = 'Red'
                  OR p.color = 'Blue' )
           AND sp.qty > 200
    GROUP  BY p.pno,
              p.weight,
              p.color
    HAVING Sum(qty) > 350
    ORDER  BY  Max(sp.qty)";

    // Invoke  and get result in one call
    //
    let outer = || -> Result<Table, String> {
        let chained_result = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?
        .table_join(
            Some("join".to_string()),
            Params2Join {
                //right_table: &Table::from_csv("supplierpart", dirpath, "supplierpart.csv", None, None),
                right_table: &Table::from_csv(Params2FromCSV {
                    table_name: "supplierpart",
                    dir: dirpath,
                    file: "supplierpart.csv",
                    filter: None,
                    project: None,
                })?,
                joinkindandcols: None,
                cond_left: None,
                cond_right: None,
                cond_both: Some(
                    |left: &Table,
                     right: &Table,
                     leftrow: &TableRow,
                     rightrow: &TableRow|
                     -> Result<bool,String> {
                        let colindex_left = left.colname_2_index("PNO")?;
                        let colindex_right = right.colname_2_index("PNO")?;
                        Ok(leftrow[colindex_left] == rightrow[colindex_right])
                    },
                ),
                project: None,
            },
        )?
        .table_groupby(
            Some("aggregate".to_string()),
            Params2GroupBy {
                out_table: "Finally",
                groupon: Some(vec!["parts.PNO", "parts.WEIGHT", "parts.COLOR"]),
                aggrcols: vec![
                    vec!["max", "supplierpart.QTY", "Max supplierpart.QTY"], // aggr func and colname!
                    vec!["sum", "supplierpart.QTY", "Sum supplierpart.QTY"],
                ],
                custom_aggr: None,
                cond_having: Some(|_obj: &Table, row: &TableRow| -> bool {
                    let mysubvariaindex_sumqty = 1;
                    row[mysubvariaindex_sumqty].parse::<f32>().unwrap() > 350.0
                }),
            },
        )?
        .table_orderby(
            Some("order by".to_string()),
            Params2OrderBy {
                order_cols: Some(vec!["Max supplierpart.QTY"]),
                ordering_callback: None,
            },
        );
        chained_result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_query' : {}", err);
        }
    }
}
