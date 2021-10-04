use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_join::{Params2Join, PostProject};
//use tableclass::table_folder::table_select::Params2Select;
//use tableclass::table_folder::table_limit::Params2Limit;

pub fn ex_join_and_project(dirpath: &str) {
    //
    // FROM S, P, SP
    //
    /*
    1. Try to reduce the number of columns as early as possible
    2. Columns are used for join and other conditions and / or as final result
    3. Be careful on join on self
    */
    let the_sql = "
    SELECT DISTINCT S.SNO,P.PNO,P.NAME,P.CITY as PartCity,S.SNAME, S.CITY as SupplierCity 
     FROM SP,P,S
    WHERE 
     P.PNO = SP.PNO
     AND
     S.SNO = SP.SNO
    ";
    let outer = || -> Result<Table, String> {
        let _outline = Table::table_from_dummy()
            .table_from_outline("FROM CSV supplierpart", "Params")
            .table_join_outline("FROM CSV part ON P.PNO = SP.PNO", "Params")
            .table_join_outline("FROM CSV supplier ON S.SNO = SP.SNO", "Params")
            .table_select_outline(
                "SELECT S.SNAME,P.PNAME,S.CITY as SupplierCity,P.CITY as PartCity",
                "Params",
            )
            .table_distinct_outline("SELECT distinct ", "Params");

        //    eprintln!("{}", outline.show());

        let supplierpart = Table::from_csv(Params2FromCSV {
            table_name: "1",
            dir: dirpath,
            file: "supplierpart.csv",
            filter: None,
            project: None,
        })?;
        let parts = Table::from_csv(Params2FromCSV {
            table_name: "2",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?;

        let supplier = Table::from_csv(Params2FromCSV {
            table_name: "3",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?;

        let join_1_2 = supplierpart.table_join(
            Some("FROM CSV supplierpart ON P.PNO = SP.PNO".to_string()),
            Params2Join {
                right_table: &parts,
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
                project: Some(&PostProject {
                    table_name: Some("1 Joined 2"),
                    leftcols: Some(vec![("SNO", Some("SNO")), ("PNO", Some("PNO"))]),
                    rightcols: Some(vec![("PNAME", Some("PNAME")), ("CITY", Some("PartCity"))]),
                }),
            },
        );
        let join_1_2_3 = join_1_2?.table_join(
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
                     -> Result<bool,String> {
                        let colindex_left = left.colname_2_index("SNO")?;
                        let colindex_right = right.colname_2_index("SNO")?;
                        Ok(leftrow[colindex_left] == rightrow[colindex_right])
                    },
                ),
                project: Some(&PostProject {
                    table_name: Some("1 Joined 2 joined 3"),
                    leftcols: Some(vec![
                        ("SNO", Some("SNO")),
                        ("PNO", Some("PNO")),
                        ("PNAME", Some("PNAME")),
                        ("PartCity", Some("PartCity")),
                    ]),
                    rightcols: Some(vec![
                        ("SNAME", Some("SNAME")),
                        ("CITY", Some("SupplierCity")),
                    ]),
                }),
            },
        );
        let result = join_1_2_3?.table_distinct(Some("".to_string()));
        result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_join_and_project' : {}", err);
        }
    }
}
