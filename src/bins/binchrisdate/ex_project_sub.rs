use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_select::Params2Select;
use tableclass::table_folder::table_select_sub::{Params2SelectSub, SelectionKind};
use tableclass::table_folder::table_where::{Params2Where, SimpleOrComplex};

// part holds "PNO","PNAME","COLOR","WEIGHT","CITY"
// supplier holds "SNO","SNAME","STATUS","CITY"
// supplierpart holds "SNO","PNO","QTY"

pub fn ex_project_sub(dirpath: &str) {
    //
    // PROJECT with subquery
    //
    let the_sql = "SELECT 'Totals',
    SNO, 
    (SELECT SNAME FROM S WHERE S.SNO = SP.SNO) AS SNAME,
    PNO,
    (SELECT PNAME FROM P WHERE P.PNO = SP.PNO) AS PNAME,
    QTY
 FROM SP
";
    let col3_subquery = |superobj: &Table,
                         superrow: &TableRow,
                         extra_tables: &Vec<&Table>|
     -> Result<Table, String> {
        //SELECT SNAME FROM S WHERE S.SNO = SP.SNO
        let table_to_use = extra_tables[0];

        if extra_tables.len() > 0 && table_to_use.table_name == "supplier" {
            table_to_use
                .table_where(
                    Some("WHERE S.SNO =SP.SNO".to_string()),
                    Params2Where {
                        super_obj: Some(superobj),
                        super_row: Some(superrow.to_vec()),
                        simple_or_complex: SimpleOrComplex::SimpleCond(
                            |obj: &Table,
                             row: &TableRow,
                             super_obj: Option<&Table>,
                             super_row: Option<TableRow>|
                             -> Result<bool, String> {
                                let colindex_super_sno = super_obj.unwrap().colname_2_index("SNO")?;
                                let colindex_obj_sno = obj.colname_2_index("SNO")?;
                                Ok(row[colindex_obj_sno] == super_row.unwrap()[colindex_super_sno])
                            },
                        ),
                    },
                )?
                .table_select(
                    Some("SELECT SNAME".to_string()),
                    Params2Select {
                        from_cols: vec!["SNAME"],
                        to_table_name: None,
                        to_column_names: None,
                    },
                )
        } else {
            return Err(format!("Project subquery reqiures table 'S' "));
        }
    };
    let col5_subquery = |superobj: &Table,
                         superrow: &TableRow,
                         extra_tables: &Vec<&Table>|
     -> Result<Table, String> {
        //(SELECT PNAME FROM P WHERE P.PNO = SP.PNO) AS PNAME
        let table_to_use = extra_tables[1];
        if extra_tables.len() > 0 && table_to_use.table_name == "part" {
            table_to_use
                .table_where(
                    Some("WHERE P.PNO = SP.PNO".to_string()),
                    Params2Where {
                        super_obj: Some(superobj),
                        super_row: Some(superrow.to_vec()),
                        simple_or_complex: SimpleOrComplex::SimpleCond(
                            |obj: &Table,
                             row: &TableRow,
                             super_obj: Option<&Table>,
                             super_row: Option<TableRow>|
                             -> Result<bool, String> {
                                let colindex_super_pno = super_obj.unwrap().colname_2_index("PNO")?;
                                let colindex_obj_pno = obj.colname_2_index("PNO")?;
                                Ok(row[colindex_obj_pno] == super_row.unwrap()[colindex_super_pno])
                            },
                        ),
                    },
                )?
                .table_select(
                    Some("SELECT PNAME".to_string()),
                    Params2Select {
                        from_cols: vec!["PNAME"],
                        to_table_name: None,
                        to_column_names: None,
                    },
                )
        } else {
            return Err(format!("Project subquery reqiures table 'P' "));
        }
    };
    /*--------------------------------------------------------------------------*/
    // START HERE
    // open the three datasets
    let outer = || -> Result<Table, String> {
        let supplierpart = Table::from_csv(Params2FromCSV {
            table_name: "supplierpart",
            dir: dirpath,
            file: "supplierpart.csv",
            filter: None,
            project: None,
        })?;

        let part = Table::from_csv(Params2FromCSV {
            table_name: "part",
            dir: dirpath,
            file: "part.csv",
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
        let result = supplierpart.table_select_sub(
            Some(the_sql.to_string()),
            Params2SelectSub {
                from_cols: vec![
                    SelectionKind::Literal {
                        fixed: "Totals".to_string(),
                    },
                    SelectionKind::Simple {
                        from_colname: "SNO".to_string(),
                    },
                    SelectionKind::SubQuery {
                        the_sub_query_func: col3_subquery,
                    },
                    SelectionKind::Simple {
                        from_colname: "PNO".to_string(),
                    },
                    SelectionKind::SubQuery {
                        the_sub_query_func: col5_subquery,
                    },
                    SelectionKind::Simple {
                        from_colname: "QTY".to_string(),
                    },
                ],
                to_table_name: None,
                to_column_names: vec!["Totals", "sno", "sname", "pno", "pname", "qty"],
                extra_tables: &vec![&supplier, &part],
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
            eprintln!("Some error in 'ex_project_sub' : {}", err);
        }
    }
}
