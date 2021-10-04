use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table, TableRow};
use cpu_time::ProcessTime;
use std::cmp::Ordering;

#[derive(Clone)]
pub struct Params2OrderBy<'a> {
    pub order_cols: Option<Vec<&'a str>>,
    pub ordering_callback: Option<fn(&Table, &TableRow, &TableRow) -> Ordering>,
}

impl Table {
    /*--------------------------------------------------------*/
    pub fn table_orderby(
        &self,
        sqlstm: Option<String>,
        par: Params2OrderBy,
    ) -> Result<Table, String> {
        let start = ProcessTime::now();
        if (par.order_cols.is_some() && par.ordering_callback.is_some())
            || (par.order_cols.is_none() && par.ordering_callback.is_none())
        {
            return Err("Order by parameter problems".to_string());
        }
        if par.order_cols.is_some() {
            let mut pick_cols_index: Vec<usize> = Vec::new();
            for from_colname in &par.order_cols.unwrap() {
                match self.headers.iter().position(|h| h == from_colname) {
                    Some(pos) => pick_cols_index.push(pos),
                    None => {
                        return Err(format!(
                            "In 'table_orderby' the column '{}' not found in table '{}'",
                            from_colname, self.table_name
                        ));
                    }
                }
            }

            use std::collections::BinaryHeap;
            let mut the_heap: BinaryHeap<(Vec<String>, Vec<String>)> = BinaryHeap::new();
            for arow in &self.rows {
                let the_key = pick_cols_index.iter().map(|i| arow[*i].clone()).collect();
                the_heap.push((the_key, arow.to_vec()));
            }
            let sorted_heap = the_heap.into_sorted_vec();

            let new_id = logit_gen_new_id();
            let tbl2ret = Table::new(
                &self.table_name,
                self.headers.clone(),
                sorted_heap.into_iter().map(|(_k, v)| v).collect(),
                new_id,
            );
            let order_by_log = OperLog::new(
                LogOper::OrderBy,
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
            logit_collect_operlog_single(order_by_log, new_id);
            Ok(tbl2ret)
        } else {
            let mut rows_copy = self.rows.clone();
            rows_copy.sort_by(|a, b| par.ordering_callback.unwrap()(&self, a, b));
            let new_id = logit_gen_new_id();
            let tbl2ret = Table::new(
                &self.table_name.clone(),
                self.headers.clone(),
                rows_copy,
                new_id,
            );
            let order_by_log = OperLog::new(
                LogOper::OrderBy,
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
            logit_collect_operlog_single(order_by_log, new_id);
            Ok(tbl2ret)
        }
    }
}
