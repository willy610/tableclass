use crate::table::{Table};

impl Table {
    pub fn table_as_csv(&self) -> String {
        let hdrs: Vec<String> = self.headers.iter().map(|c| format!("\"{}\"", c)).collect();
        let rows: Vec<String> = self
            .rows
            .iter()
            .map(|row| -> String {
                row.iter()
                    .map(|column| format!("\"{}\"", column))
                    .collect::<Vec<String>>()
                    .join(",")
            })
            .collect();
        format!("{}\n{}", hdrs.join(","), rows.join("\n"))
    }
}
