use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table};
use cpu_time::ProcessTime;

#[derive(Clone, Debug)]
pub struct Params2Limit {
    pub offset: Option<usize>,
    pub max_number_rows: Option<usize>,
}

impl Table {
    pub fn table_limit(&self, sqlstm: Option<String>, par: Params2Limit) -> Result<Table, String> {
        // We want to take a slice like self.rows[start..end];
        // The range start..end contains all values with start <= x < end.
        // It is empty if start >= end.

        // [0..0] is item[0]
        // [0..1] is item[0]
        // [0..2] is item[0] and item[1]
        // [0..n] is item[0] up to including item[n-1]

        let start = ProcessTime::now();
        let (from_incl, to_excl) = match (par.offset, par.max_number_rows) {
            (Some(start), Some(maxnr)) => (start - 0, start + maxnr - 1),
            (Some(start), None) => (start - 0, self.rows.len()),
            (None, Some(maxnr)) => (0, maxnr - 1),
            (None, None) => (0, self.rows.len()),
        };
        if from_incl > self.rows.len() - 1 {
            return Err(format!(
                "table_limit, limit start must be in (1,{:?}) is {:?}",
                self.rows.len() - 1,
                from_incl
            ));
        }
        if to_excl > self.rows.len() {
            return Err(format!(
                "table_limit, max_number_rows (1,{:?}) is {:?}",
                self.rows.len() - 1,
                to_excl
            ));
        }
        if from_incl > to_excl {
            return Err(format!(
                "table_limit, range is negative, range=({:?},{:?})",
                from_incl, to_excl
            ));
        }
        let new_id = logit_gen_new_id();

        let tbl2ret = Table::new(
            &self.table_name,
            self.headers.clone(),
            self.rows[from_incl..to_excl].to_vec(),
            new_id,
        );
        let limit_log = OperLog::new(
            LogOper::Limit,
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
        logit_collect_operlog_single(limit_log, new_id);
        Ok(tbl2ret)
    }
}
