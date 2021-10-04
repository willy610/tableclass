use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_select::Params2Select;

pub fn ex_join(dirpath: &str) {
    /*--------------------------------------------------------*/
    /*--------------------------------------------------------*/
    // JOIN
    let the_sql = "SELECT parts.PNO,supplier.SNO
    FROM P JOIN S ON 
        P.WEIGHT LessOrEqual S.STATUS
        AND P.COLOR = 'Blue' and S.STATUS = 20";
    /*--------------------------------------------------------*/
    /*--------------------------------------------------------*/
    //
    let outer = || -> Result<Table, String> {
        let _outline = Table::table_from_dummy()
            .table_from_outline("FROM CSV parts", "Params")
            .table_join_outline(
                "JOIN S ON P.WEIGHT <= S.STATUS AND AND P.COLOR = 'Blue' and S.STATUS = 20 ",
                "Params",
            )
            .table_select_outline("parts.PNO,supplier.SNO", "Params");

        let cond_left = |obj: &Table, row: &TableRow| -> Result<bool,String> {
            let colindex_color = obj.colname_2_index("COLOR")?;
            Ok(row[colindex_color] == "Blue")
        };
//        let cond_left = cond_left?;
        let cond_right = |obj: &Table, row: &TableRow| -> Result<bool,String> {
            let colindex_status = obj.colname_2_index("STATUS")?;
            Ok(row[colindex_status] == "20")
        };
        let cond_both =
            |left: &Table, right: &Table, leftrow: &TableRow, rightrow: &TableRow| -> Result<bool,String> {
                let colindex_left = left.colname_2_index("WEIGHT");
                let colindex_right = right.colname_2_index("STATUS");
                Ok(leftrow[colindex_left?] <= rightrow[colindex_right?])
            };

        let result = Table::from_csv(Params2FromCSV {
        table_name: "parts",
        dir: dirpath,
        file: "part.csv",
        filter: None,
        project: None,
    })?
    .table_join(
        Some("ON parts.WEIGHT 'less or equal' supplier.STATUS AND parts.COLOR='Blue' AND supplier.STATUS ='20'".to_string()),
        Params2Join {
            right_table: &Table::from_csv(Params2FromCSV {
                table_name: "supplier",
                dir: dirpath,
                file: "supplier.csv",
                filter: None,
                project: None,
            })?,
            joinkindandcols: None,
            cond_left: Some(cond_left),
            cond_right: Some(cond_right),
            cond_both: Some(cond_both),
            project:None
        })?
        .table_select(
            Some("SELECT parts.PNO, supplier.SNO".to_string()),
            Params2Select {
                from_cols: vec!["parts.PNO", "supplier.SNO"],
                to_table_name: None,
                to_column_names: None,
            },
        );
        //
//        eprintln!("{}\n{}", sql, result.show());
        //    explain(&result, sql);
        result
    };
    match  outer(){
        Ok(result_set)=>{
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational 
        }
        Err(err)=>{eprintln!("Some error in 'ex_distinct' : {}", err);}
    }
}
