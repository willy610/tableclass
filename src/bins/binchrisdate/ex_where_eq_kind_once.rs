use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_select::Params2Select;
use tableclass::table_folder::table_where::{
    OnceOrDepSub, Params2Where, SimpleOrComplex, SubOrOnceAndCheck,
};

/*--------------------------------------------------------*/
// select .. where x =  (once SUBQUERY )
//
/*
SELECT *
FROM S
WHERE S.city =
    (SELECT S.CITY
     FROM S
     WHERE S.SNO = 'S101')
*/
/*--------------------------------------------------------------------------------*/
pub fn ex_where_eq_kind_once(dirpath: &str) {
    //
    // DERIVED TABLE AS SUBQUERY. ONCE INVOKATION OF SUBQ
    //
    let the_sql = "SELECT *
    FROM S
    WHERE S.city =
     (SELECT S.CITY
      FROM S
       WHERE S.SNO = 'S101')
   ";
   
    let outer = || -> Result<Table, String> {
        let subq_once = Table::from_csv(Params2FromCSV {
            table_name: "supplier",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?
        .table_where(
            Some("SELECT S.CITY FROM S WHERE S.SNO = 'S101'".to_string()),
            Params2Where {
                super_obj: None,
                super_row: None,
                simple_or_complex: SimpleOrComplex::SimpleCond(
                    |subobj: &Table,
                     subrow: &TableRow,
                     _superobj: Option<&Table>,
                     _superrow: Option<TableRow>|
                     -> Result<bool, String> {
                        let colindex_sub_sno = subobj.colname_2_index("SNO")?;
                        Ok( subrow[colindex_sub_sno] == "S101".to_string())
                    },
                ),
            },
        )?
        .table_select(
            Some("SELECT S.CITY".to_string()),
            Params2Select {
                from_cols: vec!["CITY"],
                to_table_name: None,
                to_column_names: None,
            },
        )?;
        //
        // Test for euqility S.city = (...)
        //

        let par_where = Params2Where {
            super_obj: None,
            super_row: None,
            simple_or_complex: SimpleOrComplex::ComplexCond(SubOrOnceAndCheck {
                once_or_sub: OnceOrDepSub::OnceSub(subq_once),
                the_eval_subq_result_function: |superobj: &Table,
                                                superrow: &TableRow,
                                                thesubqoutcome: &Table|
                 -> Result<bool, String> {
                    // Test for euqility S.city = (...)
                    if thesubqoutcome.rows.len() == 1 {
                        Ok(superrow[superobj.colname_2_index("CITY")?]
                            == thesubqoutcome.rows[0][thesubqoutcome.colname_2_index("CITY")?])
                    } else {
                        return Err(
                            "In 'ex_where_eq_kind_once' Result set must hold exact one row"
                                .to_string(),
                        );

                    }
                },
            }),
        };
        //
        // START HERE
        //
        let supplier = Table::from_csv(Params2FromCSV {
            table_name: "supplier",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?;

        let result = supplier.table_where(Some("WHERE S.CITY = P.CITY".to_string()), par_where);
        //    eprintln!("{}\n{}", the_sql, result.show());
        //    explain(&result, the_sql);
        result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_where_eq_kind_once' : {}", err);
        }
    }
}
