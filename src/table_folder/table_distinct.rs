use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table, TableRow};
use cpu_time::ProcessTime;
use std::collections::HashSet;

impl Table {
    /*--------------------------------------------------------*/
    pub fn table_distinct(&self, sqlstm: Option<String>) -> Result<Table, String> {
        let start = ProcessTime::now();

        let result_set: HashSet<TableRow> = self.rows.iter().map(|row| row.clone()).collect();

        let new_id = logit_gen_new_id();

        let tbl2ret = Table::new(
            &self.table_name,
            self.headers.clone(),
            result_set.into_iter().map(|row| row).collect(),
            new_id,
        );
        let distinct_log = OperLog::new(
            LogOper::Distinct,
            self.table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            sqlstm,
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Derived,
                intable_name: self.table_name.to_string(),
                intable_id: Some(self.get_id()),
            }],
        );
        logit_collect_operlog_single(distinct_log, new_id);
        Ok(tbl2ret)
    }
}
