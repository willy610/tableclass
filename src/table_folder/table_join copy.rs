use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table, TableRow, TableRows};
use cpu_time::ProcessTime;

#[derive(Clone, Debug)]
pub struct JoinKindAndCols<'a> {
    pub joinhow: &'a str, //INNER, LEFT, FULL, UNION?
    pub joincols: Vec<Vec<&'a str>>,
}
#[derive(Clone, Debug)]
pub struct PostProject<'a> {
    pub table_name: Option<&'a str>, // New name
    // cols from to [("from",Some("to")),...]
    pub leftcols: Option<Vec<(&'a str, Option<&'a str>)>>,
    pub rightcols: Option<Vec<(&'a str, Option<&'a str>)>>,
}
//#[derive(Clone, Debug)]
#[derive(Clone)]
pub struct Params2Join<'a> {
    pub right_table: &'a Table,
    pub joinkindandcols: Option<JoinKindAndCols<'a>>,
    pub cond_left: Option<fn(&Table, &TableRow) -> bool>,
    pub cond_right: Option<fn(&Table, &TableRow) -> bool>,
    pub cond_both: Option<fn(&Table, &Table, &TableRow, &TableRow) -> bool>,
    pub project: Option<&'a PostProject<'a>>,
}

impl Table {
    pub fn table_join(&self, sqlstm: Option<String>, par: Params2Join) -> Table {
        use std::collections::hash_map::Entry::{Occupied, Vacant};
        use std::collections::HashMap;
        #[derive(Debug)]
        struct DatatTillNycklar {
            left: Vec<usize>,
            right: Vec<usize>,
        }
        let start = ProcessTime::now();
        type KeyGroup = Vec<String>;
        type DataGroup = DatatTillNycklar;
        let mut group_on_join: HashMap<KeyGroup, DataGroup> = HashMap::new();
        let mut final_rows: TableRows = Vec::new();

        /*
            1. There might be some projection
            2. So build pick index for values to pick from row to final_rows[]
            3. Also build proper colnames and table name
        */
        let _default_left_colums = || -> Vec<String> {
            self.headers
                .iter()
                .map(|x| format!("{}.{}", self.table_name, x))
                .collect()
        };
        let _default_right_colums = || -> Vec<String> {
            par.right_table
                .headers
                .iter()
                .map(|x| format!("{}.{}", par.right_table.table_name, x))
                .collect()
        };
        let left_cols: Vec<String> = self
            .headers
            .iter()
            .map(|x| format!("{}.{}", self.table_name, x))
            .collect();
        let havinproject_row = false;
        let pick_index_finalrow = vec![1, 3, 0];
        // Left
        // 1 Produce colnames
        // 2 Produce indec for pick
        let colnames_and_index = |obj: &Table,
                                  left_or_right: &Option<Vec<(&str, Option<&str>)>>|
         -> (Vec<String>, Vec<usize>) {
            let result = match left_or_right {
                Some(from_to_array) => {
                    let mut new_colnames: Vec<String> = Vec::new();
                    let mut pickindex: Vec<usize> = Vec::new();
                    for (from_col_name, to_colname) in from_to_array {
                        eprintln!("{}", from_col_name);
                        let index_column =
                            obj.headers.iter().position(|r| r == from_col_name).unwrap();
                        pickindex.push(index_column);
                        match to_colname {
                            Some(name) => new_colnames.push(name.to_string()),
                            None => new_colnames.push(from_col_name.to_string()),
                        }
                    }
                    (new_colnames, pickindex)
                }
                None => {
                    // default
                    (obj.headers.clone(), (0..obj.headers.len()).collect())
                }
            };
            result
        };
        let mut out_table_name = format!("{}_{}", self.table_name, par.right_table.table_name);
        out_table_name = match par.project {
            None => out_table_name,
            Some(PostProject { table_name, .. }) => match table_name {
                Some(user_choice) => user_choice.to_string(),
                None => out_table_name,
            },
        };

        match Some(par.project) {
            None => {
//                let l = _default_left_colums();
                let left = colnames_and_index(&self, &None);
//                let r = _default_right_colums();
                let right = colnames_and_index(&par.right_table, &None);
                let final_colnames: Vec<String> =
                    l.iter().cloned().chain(r.iter().cloned()).collect();
                let final_pick_col_index = 0..(l.len() + r.len());
            }
            Some(some_project) => {
                let left = match some_project {
                    Some(PostProject { leftcols, .. }) => {
                        colnames_and_index(&self, leftcols)
                    }
                    None => {
                        colnames_and_index(&self, &None)
                    }
                };
                eprintln!("left={:?}", left);

                let right = match some_project {
                    Some(PostProject { rightcols, .. }) => {
                        colnames_and_index(&par.right_table, rightcols)
                    }
                    None => {
                        colnames_and_index(&par.right_table, &None)
                    }
                };
                eprintln!("right={:?}", right);
            }
        }

        let right_cols: Vec<String> = par
            .right_table
            .headers
            .iter()
            .map(|x| format!("{}.{}", par.right_table.table_name, x))
            .collect();

        let project_final_row = |finalrow: TableRow| -> TableRow {
            if havinproject_row {
                pick_index_finalrow
                    .iter()
                    .map(|i| finalrow[*i as usize].clone())
                    .collect()
            } else {
                finalrow
            }
        };
        //
        // Start joining rows
        //
        match par.joinkindandcols {
            Some(ref joinkindandcols) => {
                let key_left_and_right_index: Vec<(usize, usize)> = joinkindandcols
                    .joincols
                    .iter()
                    .map(|l_r| {
                        (
                            self.colname_2_index(l_r[0]),
                            par.right_table.colname_2_index(l_r[1]),
                        )
                    })
                    .collect();
                /*===========================*/
                let gen_inner = |vals: &DataGroup, final_rows: &mut TableRows| {
                    if vals.left.len() != 0 && vals.right.len() != 0 {
                        for leftrow in vals.left.iter() {
                            for rightrow in vals.right.iter() {
                                let lr = self.rows[*leftrow].clone();
                                let rr = par.right_table.rows[*rightrow].clone();

                                if par.cond_both.is_some()
                                    && par.cond_both.unwrap()(&self, &par.right_table, &lr, &rr)
                                    || par.cond_both.is_none()
                                {
                                    let new_row =
                                        lr.iter().cloned().chain(rr.iter().cloned()).collect();
                                    final_rows.push(new_row);
                                }
                            }
                        }
                    }
                };
                /*===========================*/
                let gen_left = |vals: &DataGroup, final_rows: &mut TableRows| {
                    if vals.right.len() == 0 {
                        let right_nuller = vec!["null".to_string(); par.right_table.headers.len()];

                        for leftrow in vals.left.iter() {
                            let lr = self.rows[*leftrow].clone();

                            let new_row = lr
                                .iter()
                                .cloned()
                                .chain(right_nuller.iter().cloned())
                                .collect();
                            final_rows.push(new_row);
                        }
                    }
                };
                /*===========================*/
                let gen_right = |vals: &DataGroup, final_rows: &mut TableRows| {
                    if vals.left.len() == 0 {
                        let left_nuller = vec!["null".to_string(); self.headers.len()];
                        for rightrow in vals.right.iter() {
                            let rr = par.right_table.rows[*rightrow].clone();

                            let new_row = left_nuller
                                .iter()
                                .cloned()
                                .chain(rr.iter().cloned())
                                .collect();
                            final_rows.push(new_row);
                        }
                    }
                };
                /*===========================*/
                for (_rowindex, leftrow) in (&self.rows).iter().enumerate() {
                    if par.cond_left.is_some() && par.cond_left.unwrap()(&self, &leftrow)
                        || par.cond_left.is_none()
                    {
                        let the_key: Vec<String> = key_left_and_right_index
                            .iter()
                            .map(|l_r| leftrow[l_r.0].clone())
                            .collect();
                        let val = match group_on_join.entry(the_key.clone()) {
                            Vacant(entry) => {
                                let new_datat: DataGroup = DataGroup {
                                    left: Vec::new(),
                                    right: Vec::new(),
                                };
                                entry.insert(new_datat)
                            }
                            Occupied(entry) => entry.into_mut(),
                        };
                        //val.left.push(leftrow.to_vec());
                        val.left.push(_rowindex);
                    }
                }
                for (_rowindex, rightrow) in (&par.right_table.rows).iter().enumerate() {
                    if par.cond_right.is_some()
                        && par.cond_right.unwrap()(&par.right_table, &rightrow)
                        || par.cond_right.is_none()
                    {
                        let the_key: Vec<String> = key_left_and_right_index
                            .iter()
                            .map(|l_r| rightrow[l_r.1].clone())
                            .collect();
                        let val = match group_on_join.entry(the_key.clone()) {
                            Vacant(entry) => {
                                let new_datat: DataGroup = DataGroup {
                                    left: Vec::new(),
                                    right: Vec::new(),
                                };
                                entry.insert(new_datat)
                            }
                            Occupied(entry) => entry.into_mut(),
                        };
                        val.right.push(_rowindex);
                    }
                }
                for (ref _key, ref vals) in group_on_join.into_iter() {
                    let DataGroup { left: _, right: _ } = &vals;
                    match joinkindandcols.joinhow {
                        "INNER" => {
                            gen_inner(&vals, &mut final_rows);
                        }
                        "LEFT" => {
                            gen_left(&vals, &mut final_rows);
                            gen_inner(&vals, &mut final_rows);
                        }
                        "OUTER" => {
                            gen_left(&vals, &mut final_rows);
                            gen_inner(&vals, &mut final_rows);
                            gen_right(&vals, &mut final_rows);
                        }
                        _ => {
                            panic!("join must be 'INNER', 'LEFT' or 'OUTER'")
                        }
                    }
                }
            }
            None => {
                if par.cond_left.is_some() || par.cond_right.is_some() || par.cond_both.is_some() {
                    // full product (with filter)
                    for leftrow in self.rows.iter() {
                        if par.cond_left.is_some() && par.cond_left.unwrap()(&self, &leftrow)
                            || par.cond_left.is_none()
                        {
                            for rightrow in par.right_table.rows.iter() {
                                if par.cond_right.is_some()
                                    && par.cond_right.unwrap()(&par.right_table, &rightrow)
                                    || par.cond_right.is_none()
                                {
                                    if par.cond_both.is_some()
                                        && par.cond_both.unwrap()(
                                            &self,
                                            &par.right_table,
                                            &leftrow,
                                            &rightrow,
                                        )
                                        || par.cond_both.is_none()
                                    {
                                        let new_row = leftrow
                                            .iter()
                                            .cloned()
                                            .chain(rightrow.iter().cloned())
                                            .collect();
                                        final_rows.push(new_row);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    panic!("table_join: no join option found");
                }
            }
        }

        let new_id = logit_gen_new_id();

        let tbl2ret = Table::new(
            &out_table_name,
            left_cols
                .iter()
                .cloned()
                .chain(right_cols.iter().cloned())
                .collect(),
            final_rows,
            new_id,
        );
        let join_log = OperLog::new(
            //            "join".to_string(),
            LogOper::Join,
            out_table_name.clone(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            sqlstm,
            vec![
                LogSourceType {
                    sor_or_deriv: SourceOrDerived::Derived,
                    intable_name: self.table_name.to_string(),
                    intable_id: Some(self.get_id()),
                },
                LogSourceType {
                    sor_or_deriv: SourceOrDerived::Derived,
                    intable_name: par.right_table.table_name.to_string(),
                    intable_id: Some(par.right_table.get_id()),
                },
            ],
        );

        logit_collect_operlog_single(join_log, new_id);

        tbl2ret
    }
}
