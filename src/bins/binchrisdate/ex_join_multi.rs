use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_select::Params2Select;

pub fn ex_join_multi(dirpath: &str) {
    //
    // FROM S, P, SP
    //
    let the_sql = "
    SELECT DISTINCT S.SNAME,P.PNAME,S.CITY as SupplierCity,P.CITY as PartCity FROM S,SP,P
    WHERE SP.PNO = P.PNO
    AND SP.SNO = S.SNO
    ";
    let outer = || -> Result<Table, String> {
        let outline = Table::table_from_dummy()
            .table_from_outline("FROM CSV supplier", "Params")
            .table_join_outline("FROM CSV supplierpart ON SP.PNO = P.PNO", "Params")
            .table_join_outline("FROM CSV part ON SP.SNO = S.SNO", "Params")
            .table_select_outline(
                "SELECT S.SNAME,P.PNAME,S.CITY as SupplierCity,P.CITY as PartCity",
                "Params",
            )
            .table_distinct_outline("SELECT distinct ", "Params");

        eprintln!("{}", outline.show());

        let parts = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?;

        let supplierpart = Table::from_csv(Params2FromCSV {
            table_name: "supplierpart",
            dir: dirpath,
            file: "supplierpart.csv",
            filter: None,
            project: None,
        })?;

        let supplier = Table::from_csv(Params2FromCSV {
            table_name: "supplier",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?;
        let result = parts
            .table_join(
                Some("FROM CSV supplierpart ON P.PNO = SP.PNO".to_string()),
                Params2Join {
                    right_table: &supplierpart,
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
            .table_join(
                Some("FROM CSV part  ON SP.SNO = P.PNO".to_string()),
                Params2Join {
                    right_table: &supplier,
                    joinkindandcols: None,
                    cond_left: None,
                    cond_right: None,
                    cond_both: Some(
                        |left: &Table,
                         right: &Table,
                         leftrow: &TableRow,
                         rightrow: &TableRow|
                         -> Result<bool,String>  {
                            let colindex_left = left.colname_2_index("supplierpart.SNO")?;
                            let colindex_right = right.colname_2_index("SNO")?;
                            Ok(leftrow[colindex_left] == rightrow[colindex_right])
                        },
                    ),
                    project: None,
                },
            )?
            .table_select(
                Some(
                    "SELECT S.SNAME,P.PNAME,S.CITY as SupplierCity,P.CITY as PartCity".to_string(),
                ),
                Params2Select {
                    from_cols: vec![
                        "supplier.SNAME",
                        "parts_supplierpart.parts.PNAME",
                        "supplier.CITY",
                        "parts_supplierpart.parts.CITY",
                    ],
                    to_table_name: None,
                    to_column_names: Some(vec!["sname", "pname", "SupplierCity", "PartCity"]),
                },
            )?
            .table_distinct(Some("distinct".to_string()));
        result
    };
    match outer() {
        Ok(result_set) => {
            //            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_distinct' : {}", err);
        }
    }
}
