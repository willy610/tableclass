use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_where::{
    OnceOrDepSub, Params2Where, SimpleOrComplex, SubOrOnceAndCheck,
};

/*
HORRIBLE EXAMPLE!

pick all rows from SP which is related to a certain supplier

SELECT *
FROM SP
WHERE 'S101'=
(SELECT SNO
     FROM S
     WHERE S.SNO = SP.SNO)

+------+------+-----+
| SNO  | PNO  | QTY |
+------+------+-----+
| S101 | P865 | 809 |
| S101 | P872 | 101 |
+------+------+-----+

*/
/*--------------------------------------------------------------------------------*/
pub fn ex_where_eq_kind_depsubq(dirpath: &str) {
    //
    // This is the function to be called for generating the rows in the subquery
    // It's called for each row in the outer subobj (SP here)
    //
    /*--------------------------------------------------------------------------------*/
    fn my_dep_sub(
        superobj: &Table,
        superrow: &TableRow,
        tables: &Vec<&Table>,
    ) -> Result<Table, String> {
        /*
                SELECT SNO
                FROM S
                WHERE S.SNO = SP.SNO
        */
        //tables[0] holds supplier
        //
        // Evaluate this subquery for each call from outside
        //
        tables[0].table_where(
            Some("WHERE S.SNO = SP.SNO".to_string()),
            Params2Where {
                super_obj: Some(superobj),
                super_row: Some(superrow.to_vec()),
                simple_or_complex: SimpleOrComplex::SimpleCond(
                    //    S.SNO = SP.SNO
                    |obj: &Table,
                     row: &TableRow,
                     superobj: Option<&Table>,
                     superrow: Option<TableRow>|
                     -> Result<bool, String> {
                        match superobj {
                            Some(superobj) => match superrow {
                                Some(superrow) => {
                                    Ok(superrow[superobj.colname_2_index("SNO")?]
                                        == row[obj.colname_2_index("SNO")?])
                                }
                                None => {
                                    return Err("In 'ex_where_eq_kind_depsubq' Expected superrow missing".to_string())
                                }
                            },
                            None => {
                                return Err("In 'ex_where_eq_kind_depsubq' Expected superobj missing".to_string())
                            }
                        }
                    },
                ),
            },
        )
    }

    /*--------------------------------------------------------------------------------*/
    //
    // START HERE
    //
    let the_sql = "SELECT *
 FROM SP
 WHERE 'S101'=
  (SELECT SNO
   FROM S
     WHERE S.SNO = SP.SNO)";

    let outer = || -> Result<Table, String> {
        let supplier = Table::from_csv(Params2FromCSV {
            table_name: "supplier",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?;

        let the_extra_tables = vec![&supplier]; // to be used in the subquery

        let supplierpart = Table::from_csv(Params2FromCSV {
            table_name: "supplierpart",
            dir: dirpath,
            file: "supplierpart.csv",
            filter: None,
            project: None,
        })?;

        let result = supplierpart.table_where(
            Some("WHERE 'S101'=".to_string()),
            Params2Where {
                super_obj: None,
                super_row: None,
                simple_or_complex: SimpleOrComplex::ComplexCond(SubOrOnceAndCheck {
                    once_or_sub: OnceOrDepSub::ADependentSubQuery {
                        the_sub_query_func: my_dep_sub,
                        extra_tables: &the_extra_tables,
                    },
                    // equality example 'S101'= ()
                    the_eval_subq_result_function: |obj: &Table,
                                                    _row: &TableRow,
                                                    thesubqoutcome: &Table|
                     -> Result<bool, String> {
                        if thesubqoutcome.rows.len() == 1 {
                            if thesubqoutcome.rows[0][obj.colname_2_index("SNO")?] == "S101" {
                                return Ok(true);
                            } else {
                                return Ok(false);
                            }
                        } else {
                            return Err(
                                "In 'ex_where_eq_kind_depsubq' Result set must hold exact one row"
                                    .to_string(),
                            );
                        }
                    },
                }),
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
            eprintln!("Some error in 'ex_where_eq_kind_depsubq' : {}", err);
        }
    }
}
