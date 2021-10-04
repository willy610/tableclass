use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, SubQueryFunction, Table};
use cpu_time::ProcessTime;

#[derive(Clone)]
pub struct Params2WithRecursive<'a> {
    pub depsubq: SubQueryFunction,
    pub extra_tables: &'a Vec<&'a Table>,
}

impl Table {
    /*--------------------------------------------------------*/
    pub fn table_with_recursive(
        &self,
        sqlstm: Option<String>,
        par: Params2WithRecursive,
    ) -> Result<Table, String> {
        let start = ProcessTime::now();

        let mut new_table = self.clone();
        let mut last_result_row = new_table.rows[0].clone();
        //        eprintln!("{:?}", last_result_row);
        /*
        1. call subquery()
        N. loop
        if result is empty we are done
        else append all rows to final table
        let last_result_row = last on new
        */
        let mut limit = 100;
        loop {
            let result = (par.depsubq)(self, &last_result_row, par.extra_tables)?;

            for arow in &result.rows {
                new_table.rows.push(arow.to_vec());
            }
            limit = limit - 1;
            if limit < 0 {
                return Err("With Recursive terminated after 100 calls".to_string());
            }
            if result.rows.len() == 0 {
                break;
            }
            last_result_row = new_table.rows[new_table.rows.len() - 1].clone();
        }
        let new_id = logit_gen_new_id();
        new_table.id = new_id;
        let order_by_log = OperLog::new(
            LogOper::WithRecursive,
            new_table.table_name.to_string(),
            start.elapsed(),
            new_table.rows.len(),
            new_table.headers.len(),
            new_table.id,
            sqlstm,
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Derived,
                intable_name: self.table_name.to_string(),
                intable_id: Some(self.get_id()),
            }],
        );
        logit_collect_operlog_single(order_by_log, new_id);
        Ok(new_table)
    }
}
