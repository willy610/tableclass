use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};

use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table, TableRow};
use cpu_time::ProcessTime;

#[derive(Debug)]
pub struct FuncAndColumn {
    pub a_func: String,
    pub index_in_aggr: usize,
    pub index_in_row: usize,
}

#[derive(Clone, Debug)]
pub struct ForEachGroup {
    pub cnt: i32,
    pub aggr_values: TableRow,
}
#[derive(Clone, Debug)]
pub struct AggrValues {
    pub first_call: bool,
    pub aggr: ForEachGroup,
}

#[derive(Clone)]
pub struct Params2GroupBy<'a> {
    pub out_table: &'a str,
    pub groupon: Option<Vec<&'a str>>,
    pub aggrcols: Vec<Vec<&'a str>>, // triple of [func, incolname , outcolname]
    pub custom_aggr: Option<fn(&Table, &TableRow, &AggrValues) -> Result<AggrValues, String>>,
    pub cond_having: Option<fn(&Table, &TableRow) -> bool>,
}

impl Table {
    /*--------------------------------------------------------*/
    pub fn table_groupby(
        &self,
        sqlstm: Option<String>,
        par: Params2GroupBy,
    ) -> Result<Table, String> {
        use std::collections::HashMap;
        let start = ProcessTime::now();
        let mut grouped_result: HashMap<TableRow, AggrValues> = HashMap::new();

        /*-BEGIN------------------------------------------------*/
        /*- 'custom_aggr' should be like that-----------------------*/

        let all_func_and_col: Result<Vec<FuncAndColumn>, String> = par
            .aggrcols
            .iter()
            .enumerate()
            .map(|(i, aggrcols)| -> Result<FuncAndColumn, String> {
                //// aggrcols is triple of [func, incolname , outcolname]
                let func = aggrcols[0].to_string();

                let colname = aggrcols[1];
                Ok(FuncAndColumn {
                    a_func: func,
                    index_in_aggr: i,
                    index_in_row: if colname == "*" {
                        usize::MAX
                    } else {
                        self.colname_2_index(aggrcols[1])?
                    },
                })
            })
            .collect();
        let all_func_and_col = all_func_and_col?;

        let stdaggrfunc = |all_func_and_col: &Vec<FuncAndColumn>,
                           _obj: &Table,
                           row: &TableRow,
                           in_aggrvalues: &AggrValues|
         -> Result<AggrValues, String> {
            let mut accum_aggrvalues = in_aggrvalues.clone();
            if accum_aggrvalues.first_call {
                accum_aggrvalues.first_call = false;
                accum_aggrvalues.aggr.cnt = 1;
                for x in all_func_and_col {
                    if x.index_in_row == usize::MAX {
                        // is count(*)
                        accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] = "1".to_string();
                    } else {
                        accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] = row[x.index_in_row].clone();
                    }
                }
            } else {
                accum_aggrvalues.aggr.cnt = accum_aggrvalues.aggr.cnt + 1;
                for x in all_func_and_col {
                    match x.a_func.to_lowercase().as_str() {
                        "max" => {
                            let old_max: f32 =
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr].parse().unwrap();
                            let this_row_value: f32 = row[x.index_in_row].parse().unwrap();
                            if this_row_value > old_max {
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] =
                                    this_row_value.to_string();
                            }
                        }
                        "min" => {
                            let old_min: f32 =
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr].parse().unwrap();
                            let this_row_value: f32 = row[x.index_in_row].parse().unwrap();
                            if this_row_value < old_min {
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] =
                                    this_row_value.to_string();
                            }
                        }
                        "sum" => {
                            let old_sum: f32 =
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr].parse().unwrap();
                            let this_row_value: f32 = row[x.index_in_row].parse().unwrap();
                            accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] =
                                (old_sum + this_row_value).to_string();
                        }
                        "avg" => {
                            let old_avg: f32 =
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr].parse().unwrap();
                            let this_row_value: f32 = row[x.index_in_row].parse().unwrap();
                            let new_avg = (old_avg * accum_aggrvalues.aggr.cnt as f32 + this_row_value)
                                / ((accum_aggrvalues.aggr.cnt + 1) as f32);
                            accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] = new_avg.to_string();
                        }
                        "count" => {
                            let old_count: i32 =
                                accum_aggrvalues.aggr.aggr_values[x.index_in_aggr].parse().unwrap();
                            accum_aggrvalues.aggr.aggr_values[x.index_in_aggr] = (old_count + 1).to_string();
                        }
                        _ => {
                            return Err(format!(
                                "In group by the aggregate function '{}' not supported",
                                x.a_func
                            ))
                        }
                    }
                }
            }
            Ok(accum_aggrvalues.clone())
        };
        /*-END---------------------------------------------------*/

        let colnames_from_aggr: Vec<String> =
            par.aggrcols.iter().map(|c| c[2].to_string()).collect();

        match par.groupon {
            // GROUP BY something
            Some(ref grpkeys) => {
                let aggr_condens_record = AggrValues {
                    first_call: true,
                    aggr: ForEachGroup {
                        cnt: 0,
                        aggr_values: vec!["".to_string(); par.aggrcols.len()],
                    },
                };
                let group_key_index: Result<Vec<usize>, String> = grpkeys
                    .iter()
                    .map(|colname| -> Result<usize, String> { Ok(self.colname_2_index(colname)?) })
                    .collect();
                let group_key_index = group_key_index?;
                for arow in &self.rows {
                    let key: Vec<String> =
                        group_key_index.iter().map(|i| arow[*i].clone()).collect();
                    let res = grouped_result.get_mut(&key);
                    match res {
                        None => {
                            // First time in this group
                            let first_value = match par.custom_aggr {
                                Some(funk) => funk(&self, &arow, &mut aggr_condens_record.clone()),
                                None => stdaggrfunc(
                                    &all_func_and_col,
                                    &self,
                                    &arow,
                                    &mut aggr_condens_record.clone(),
                                ),
                            };
                            match first_value {
                                Ok(val) => {
                                    // Insert new value
                                    grouped_result.insert(key, val)
                                }
                                Err(err) => return Err(err),
                            };
                        }
                        Some(old_aggr) => {
                            // Key seen before
                            let new_value = match par.custom_aggr {
                                Some(funk) => funk(&self, &arow, old_aggr).clone(),
                                None => {
                                    stdaggrfunc(&all_func_and_col, &self, &arow, old_aggr).clone()
                                }
                            };
                            match new_value {
                                Ok(val) => {
                                    // Update old value
                                    *old_aggr = val;
                                }
                                Err(err) => return Err(err),
                            };
                        }
                    };
                }
                // All aggregation done
                let colnames_from_key: Vec<String> = group_key_index
                    .iter()
                    .map(|i| self.headers[*i].clone())
                    .collect();

                let new_id = logit_gen_new_id();

                let tbl2ret = Table::new(
                    par.out_table,
                    colnames_from_key
                        .iter()
                        .cloned()
                        .chain(colnames_from_aggr.iter().cloned())
                        .collect(),
                    grouped_result
                        .iter()
                        .filter_map(|(k, v)| {
                            if par.cond_having.is_some()
                                && par.cond_having.unwrap()(&self, &v.aggr.aggr_values)
                                || par.cond_having.is_none()
                            {
                                Some(
                                    k.iter()
                                        .cloned()
                                        .chain(v.aggr.aggr_values.iter().cloned())
                                        .collect(),
                                )
                            } else {
                                None // not having
                            }
                        })
                        .collect(),
                    new_id,
                );
                let aggregate_log = OperLog::new(
                    LogOper::GroupBy,
                    par.out_table.to_string(),
                    start.elapsed(),
                    tbl2ret.rows.len(),
                    tbl2ret.headers.len(),
                    tbl2ret.id,
                    sqlstm,
                    vec![LogSourceType {
                        sor_or_deriv: SourceOrDerived::Derived,
                        intable_name: par.out_table.to_string(),
                        intable_id: Some(self.get_id()),
                    }],
                );
                logit_collect_operlog_single(aggregate_log, new_id);

                Ok(tbl2ret)
            }
            None => {
                // No group by but some max or min or ...
                let mut aggr_condens_record = AggrValues {
                    first_call: true,
                    aggr: ForEachGroup {
                        cnt: 0,
                        aggr_values: vec!["".to_string(); par.aggrcols.len()],
                    },
                };
                for arow in &self.rows {
                    let res = match par.custom_aggr {
                        Some(funk) => funk(&self, &arow, &aggr_condens_record.clone()),
                        None => stdaggrfunc(
                            &all_func_and_col,
                            &self,
                            &arow,
                            &aggr_condens_record.clone(),
                        )
                        .clone(),
                    };
                    match res {
                        Ok(val) => aggr_condens_record = val,
                        Err(err) => return Err(err),
                    };
                }
                let new_id = logit_gen_new_id();
                let tbl2ret = Table::new(
                    par.out_table,
                    colnames_from_aggr,
                    vec![aggr_condens_record.aggr.aggr_values],
                    new_id,
                );
                let aggr_log = OperLog::new(
                    LogOper::GroupBy,
                    par.out_table.to_string(),
                    start.elapsed(),
                    tbl2ret.rows.len(),
                    tbl2ret.headers.len(),
                    tbl2ret.id,
                    sqlstm,
                    vec![LogSourceType {
                        sor_or_deriv: SourceOrDerived::Derived,
                        intable_name: par.out_table.to_string(),
                        intable_id: Some(self.get_id()),
                    }],
                );
                logit_collect_operlog_single(aggr_log, new_id);
                Ok(tbl2ret)
            }
        }
    }
}
