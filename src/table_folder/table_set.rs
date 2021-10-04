use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table, TableRow};
use cpu_time::ProcessTime;
use std::collections::HashSet;

impl Table {
    /*--------------------------------------------------------*/
    pub fn table_union(&self, right_table: &Table) -> Result<Table, String> {
        let start = ProcessTime::now();
        let mut result_set: HashSet<TableRow> = self.rows.iter().map(|r| r.clone()).collect();
        for right_row in &right_table.rows {
            result_set.insert(right_row.to_vec());
        }
        let out_table_name = format!("{}_{}", &self.table_name, &right_table.table_name);
        let new_id = logit_gen_new_id();

        let tbl2ret = Table::new(
            &out_table_name,
            self.headers.clone(),
            result_set.into_iter().map(|arow| arow).collect(),
            new_id,
        );
        let union_log = OperLog::new(
            LogOper::Union,
            self.table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            None,
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Derived,
                intable_name: self.table_name.to_string(),
                intable_id: Some(self.get_id()),
            }],
        );
        logit_collect_operlog_single(union_log, new_id);
        Ok(tbl2ret)
    }
    /*--------------------------------------------------------*/
    pub fn table_intersection(&self, right_table: &Table) -> Result<Table, String> {
        let start = ProcessTime::now();

        let mut result_set: HashSet<TableRow> = self.rows.iter().map(|r| r.clone()).collect();
        let right_set: HashSet<TableRow> = right_table.rows.iter().map(|r| r.clone()).collect();
        result_set.retain(|key| right_set.contains(key));
        let out_table_name = format!("{}_{}", &self.table_name, &right_table.table_name);
        let new_id = logit_gen_new_id();
        let tbl2ret = Table::new(
            &out_table_name,
            self.headers.clone(),
            result_set.into_iter().map(|arow| arow).collect(),
            new_id,
        );
        let intersection_log = OperLog::new(
            LogOper::Intersection,
            self.table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            None,
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Derived,
                intable_name: self.table_name.to_string(),
                intable_id: Some(self.get_id()),
            }],
        );
        logit_collect_operlog_single(intersection_log, new_id);
        Ok(tbl2ret)
    }
    /*--------------------------------------------------------*/
    pub fn table_except(&self, right_table: &Table) -> Result<Table, String> {
        let start = ProcessTime::now();
        let mut result_set: HashSet<TableRow> = self.rows.iter().map(|r| r.clone()).collect();
        for right_row in &right_table.rows {
            result_set.remove(right_row);
        }
        let out_table_name = format!("{}_{}", self.table_name, right_table.table_name);
        let new_id = logit_gen_new_id();

        let tbl2ret = Table::new(
            &out_table_name,
            self.headers.clone(),
            result_set.into_iter().map(|arow| arow).collect(),
            new_id,
        );
        let except_log = OperLog::new(
            LogOper::Except,
            self.table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            None,
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Derived,
                intable_name: self.table_name.to_string(),
                intable_id: Some(self.get_id()),
            }],
        );
        logit_collect_operlog_single(except_log, new_id);
        Ok(tbl2ret)
    }
}
