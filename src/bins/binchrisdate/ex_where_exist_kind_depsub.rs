use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_where::{
    OnceOrDepSub, Params2Where, SimpleOrComplex, SubOrOnceAndCheck,
};

/*
SELECT *
FROM S
WHERE EXISTS
    (SELECT *
     FROM SP
     WHERE SP.sno = S.sno)

*/
/*--------------------------------------------------------------------------------*/
pub fn ex_where_exist_kind_depsub(dirpath: &str) {
    fn my_dep_sub(
        superobj: &Table,
        superrow: &TableRow,
        tables: &Vec<&Table>,
    ) -> Result<Table, String> {
        /*
        (SELECT *
          FROM SP
          WHERE SP.sno = S.sno)
        */
        let sno_eq_sno = |obj: &Table,
                          row: &TableRow,
                          superobj: Option<&Table>,
                          superrow: Option<TableRow>|
         -> Result<bool, String> {
            match superobj {
                Some(superobj) => match superrow {
                    Some(superrow) => Ok(superrow[superobj.colname_2_index("SNO")?]
                        == row[obj.colname_2_index("SNO")?]),
                    None => {
                        return Err(
                            "In 'ex_where_exist_kind_depsub' Expected superrow missing".to_string()
                        );
                    }
                },
                None => {
                    return Err(
                        "In 'ex_where_exist_kind_depsub' Expected superobj missing".to_string()
                    );
                }
            }
        };
        let result = tables[0].table_where(
            Some("WHERE S.SNO = SP.SNO".to_string()),
            Params2Where {
                super_obj: Some(superobj),
                super_row: Some(superrow.to_vec()),
                simple_or_complex: SimpleOrComplex::SimpleCond(sno_eq_sno),
            },
        )?;
        Ok(result)
    }
    /*--------------------------------------------------------------------------------*/
    //
    // START HERE
    //
    let the_sql = "SELECT *
FROM S
WHERE EXISTS
    (SELECT *
        FROM SP
        WHERE SP.sno = S.sno)";
    let outer = || -> Result<Table, String> {
        let supplierpart = Table::from_csv(Params2FromCSV {
            table_name: "supplierpart",
            dir: dirpath,
            file: "supplierpart.csv",
            filter: None,
            project: None,
        })?;

        let the_extra_tables = &vec![&supplierpart]; // to be used in the subquery

        let par_where = Params2Where {
            super_obj: None,
            super_row: None,
            simple_or_complex: SimpleOrComplex::ComplexCond(SubOrOnceAndCheck {
                once_or_sub: OnceOrDepSub::ADependentSubQuery {
                    the_sub_query_func: my_dep_sub,
                    extra_tables: the_extra_tables,
                },
                // exist check
                the_eval_subq_result_function: |_obj: &Table,
                                                _row: &TableRow,
                                                thesubqoutcome: &Table|
                 -> Result<bool, String> {
                    return Ok(thesubqoutcome.rows.len() > 0);
                },
            }),
        };
        let supplier = Table::from_csv(Params2FromCSV {
            table_name: "supplier",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?;

        let result = supplier.table_where(Some("MYCKET ".to_string()), par_where);
        result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_where_exist_kind_depsub' : {}", err);
        }
    }
}
