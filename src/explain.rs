use crate::logit::{is_logit_enabled, logit_get_value};
use crate::table::*;
use floating_duration::TimeAsFloat;

enum LayOut {
    _FirstEventRight,  // Shor Right to left
    FirstEventLeft,    // Left to right
    _FirstEventTop,    // Top down
    _FirstEventBottom, // Bottom up
}
static LAYOUTORDER: LayOut = LayOut::FirstEventLeft;

use std::cmp;

const X_TO_PIXELS: f32 = 8.8; // depending on text size in CSS
const Y_TO_PIXELS: f32 = 13.8; // depending on text size
const LEFT_AND_RIGHT_MARGIN_AS_NR_LETTERS: TknCountModel = 1;
const X_BOX_SPACING_NR_LETTERS: TknCountModel = 4;
const Y_BOX_HEIGHT_NR_ROWS: f32 = 7.0;
const Y_BOX_HEIGHT_NR_PX: f32 = Y_BOX_HEIGHT_NR_ROWS * Y_TO_PIXELS;
const KONTAKT_RADIE: i32 = 4;
const OPRSVG_RELATIVE_OFFSET_X: f32 = 20.0;
const OPRSVG_RELATIVE_OFFSET_Y: f32 = 20.0;
const BOX_SPACING_Y:f32 = 10.0;

type TknCountModel = u32;
type RowCountModel = f32;
type TknXposModel = i32;
type TknYposModel = f32;
type TknXposPX = f32;
type TknYposPX = f32;
type TknPx = f32;

#[derive(Clone, Debug)]
pub struct Point {
    pub x: f32,
    pub y: f32,
}
impl Point {
    fn add(&self, other: Point) -> Point {
        Point {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }
}
#[derive(Clone, Debug)]
pub struct InOutContact {
    pub start_point: Point,
    pub end_point: Point,
    pub subq: bool,
}

#[derive(Clone, Debug)]
pub struct LeftAndRight {
    pub left: Box<OperChain>,
    pub left_nr_boxes_y: i32,
    pub right: Box<OperChain>,
    pub right_nr_boxes_y: i32,
}
#[derive(Clone, Debug)]
pub struct OneSingel {
    pub x_model: TknXposModel,
    pub y_model: TknYposModel,
    pub text_width: TknCountModel,
    pub outer_width: TknCountModel,
    pub operation: String,
    pub outtablename: String,
    pub cputime: String,
    pub nrrows: String,
    pub nrcols: String,
    pub out_rel_id: String,
    pub sqlstm: Option<String>,
    pub the_log_oper: LogOper,
}
impl OneSingel {
    fn get_center_right_contact(&self) -> Point {
        Point {
            x: (self.x_model + 1 + self.text_width as TknXposModel) as f32 * X_TO_PIXELS
                + KONTAKT_RADIE as f32,
            y: self.y_model,
        }
    }
    fn get_center_left_contact(&self) -> Point {
        Point {
            x: self.x_model as f32 * X_TO_PIXELS - KONTAKT_RADIE as f32,
            y: self.y_model,
        }
    }
}
#[derive(Clone, Debug)]
pub struct OperChain {
    pub y_real_center: f32,
    pub child_model_nr_boxes_y: i32,
    pub singels: Vec<OneSingel>,
    pub branch: Option<Vec<OperChain>>,
}
#[derive(Clone, Debug)]
pub struct MinMaxTkn {
    pub xmin_real: TknXposModel,
    pub xmax_real: TknXposModel,
    pub ymin_real: TknYposModel,
    pub ymax_real: TknYposModel,
}
#[derive(Clone, Debug)]
pub struct GraphAttributes {
    pub topleft: Point,
    pub width: TknPx,
    pub height: TknPx,
}
#[derive(Clone, Debug)]
pub struct SvgBox {
    calculated_screen_pos: Point,
    relative_offset: Point,
    calculated_transfer_to_zero: Point,
    calculated_bottom_left: Point,
    calculated_bottom_right: Point,
    the_content_attributes: GraphAttributes,
}
/*---------------------------------------------------------------------*/
impl OperChain {
    fn new() -> OperChain {
        OperChain {
            y_real_center: 0.0,
            child_model_nr_boxes_y: 0,
            singels: Vec::new(),
            branch: None,
        }
    }
    /*---------------------------------------------------------------------*/
    fn calc_x_pos(&mut self, at_x: TknXposModel) {
        // start this OperChain in at_x
        match LAYOUTORDER {
            LayOut::_FirstEventRight => {
                // NOT YET
                // increase x
                self.singels[0].x_model = at_x;
                let mut running_x = at_x + self.singels[0].outer_width as TknXposModel;
                for a_singel_i in 1..self.singels.len() {
                    self.singels[a_singel_i].x_model = running_x;
                    running_x = running_x + self.singels[a_singel_i].outer_width as TknXposModel;
                }
            }
            LayOut::FirstEventLeft => {
                // decrease x
                self.singels[0].x_model = at_x;
                let mut running_x = at_x;
                for a_singel_i in 1..self.singels.len() {
                    running_x =
                        running_x - self.singels[a_singel_i - 0].outer_width as TknXposModel;
                    self.singels[a_singel_i].x_model = running_x;
                }
                // !!!!!
                match self.branch {
                    Some(ref mut a_set_of_opercahin) => {
                        for i in 0..a_set_of_opercahin.len() {
                            let width = a_set_of_opercahin[i].singels[0].outer_width.clone()
                                as TknXposModel;
                            a_set_of_opercahin[i].calc_x_pos(running_x - width);
                        }
                    }
                    None => {}
                }
            }
            LayOut::_FirstEventTop => {
                // left and right moderate x
                // NOT YET
            }
            LayOut::_FirstEventBottom => {
                // left and right moderate x
                // NOT YET
            }
        }
    }
    /*---------------------------------------------------------------------*/
    fn calc_y_pos(&mut self, in_center_y_px: RowCountModel) {
        match LAYOUTORDER {
            LayOut::_FirstEventRight | LayOut::FirstEventLeft => {
                //
                self.y_real_center = in_center_y_px;
                for a_singel_i in 0..self.singels.len() {
                    self.singels[a_singel_i].y_model = in_center_y_px;
                }
                match self.branch {
                    Some(ref mut a_set_of_opercahin) => {
                        let total_y = Y_BOX_HEIGHT_NR_PX * self.child_model_nr_boxes_y as f32;
                        let mut running_y: RowCountModel = in_center_y_px - total_y / 2.0;
                        for i in 0..a_set_of_opercahin.len() {
                            let delta_child_y = a_set_of_opercahin[i].child_model_nr_boxes_y as f32
                                * Y_BOX_HEIGHT_NR_PX;
                            let child_y: RowCountModel = running_y + delta_child_y / 2.0;
                            a_set_of_opercahin[i].calc_y_pos(child_y);
                            running_y = running_y + delta_child_y;
                        }
                    }
                    None => {}
                };
            }
            LayOut::_FirstEventTop => {
                // NOT YET
            }
            LayOut::_FirstEventBottom => {
                // NOT YET
            }
        }
    }
    /*---------------------------------------------------------------------*/
    fn calc_mima(&mut self, in_mima: MinMaxTkn) -> MinMaxTkn {
        let mut ut_mima = in_mima.clone();
        match LAYOUTORDER {
            // NOT YET
            LayOut::_FirstEventRight => {
                let first_singel = self.singels.first().unwrap();
                let last_singel = self.singels.last().unwrap();
                ut_mima.xmin_real = cmp::min(ut_mima.xmin_real, first_singel.x_model);
                ut_mima.xmax_real = cmp::max(
                    ut_mima.xmax_real,
                    last_singel.x_model + 1 + last_singel.text_width as TknXposModel,
                );
                if self.y_real_center < ut_mima.ymin_real {
                    ut_mima.ymin_real = self.y_real_center;
                }
                if self.y_real_center > ut_mima.ymax_real {
                    ut_mima.ymax_real = self.y_real_center;
                }
            }
            LayOut::FirstEventLeft => {
                let first_singel = self.singels.first().unwrap();
                let last_singel = self.singels.last().unwrap();
                ut_mima.xmin_real = cmp::min(ut_mima.xmin_real, last_singel.x_model);
                ut_mima.xmax_real = cmp::max(
                    ut_mima.xmax_real,
                    first_singel.x_model + 1 + first_singel.text_width as TknXposModel,
                );
                if self.y_real_center < ut_mima.ymin_real {
                    ut_mima.ymin_real = self.y_real_center;
                }
                if self.y_real_center > ut_mima.ymax_real {
                    ut_mima.ymax_real = self.y_real_center;
                }
                match self.branch {
                    Some(ref mut a_set_of_opercahin) => {
                        for i in 0..a_set_of_opercahin.len() {
                            ut_mima = a_set_of_opercahin[i].calc_mima(ut_mima);
                        }
                    }
                    None => {}
                }
            }
            LayOut::_FirstEventTop => {
                // NOT YET
            }
            LayOut::_FirstEventBottom => {
                // NOT YET
            }
        };
        ut_mima
    }
    /*---------------------------------------------------------------------*/
    fn _get_real_xy_first(&mut self) -> Point {
        Point {
            x: translate_model_x_to_px(self.singels[0].x_model),
            y: self.y_real_center,
        }
    }
    /*---------------------------------------------------------------------*/
    fn _get_real_xy_last(&mut self) -> Point {
        Point {
            x: translate_model_x_to_px(self.singels.last().unwrap().x_model),
            y: self.y_real_center,
        }
    }
    /*---------------------------------------------------------------------*/
    fn gen_in_and_out_contact(&mut self) -> Vec<InOutContact> {
        let mut singel_and_left_and_right_result: Vec<InOutContact> = Vec::new();
        /*
        https://stackoverflow.com/questions/40792801/best-way-to-concatenate-vectors-in-rust
        let c: Vec<i32> = a.into_iter().chain(b.into_iter()).collect(); // Consumed
        let c: Vec<&i32> = a.iter().chain(b.iter()).collect(); // Referenced
        let c: Vec<i32> = a.iter().cloned().chain(b.iter().cloned()).collect(); // Cloned
        let c: Vec<i32> = a.iter().copied().chain(b.iter().copied()).collect(); // Copied
        */
        match self.branch {
            Some(ref mut a_set_of_opercahin) => {
                for branc_ix in 0..a_set_of_opercahin.len() {
                    let res = a_set_of_opercahin[branc_ix].gen_in_and_out_contact();
                    for a_in_out_contact in res {
                        singel_and_left_and_right_result.push(a_in_out_contact);
                        // connect last with left and right
                    }
                    match LAYOUTORDER {
                        LayOut::_FirstEventRight => { //* NOT YET
                        }
                        LayOut::FirstEventLeft => {
                            let arc_end =
                                self.singels[self.singels.len() - 1].get_center_left_contact();
                            let left_start_xy =
                                a_set_of_opercahin[branc_ix].singels[0].get_center_right_contact();
                            let mut bidirect_arc = match &self.singels[0].the_log_oper {
                                LogOper::WhereDepSub => true,
                                _ => false,
                            };
                            if branc_ix != 0 {
                                bidirect_arc = false; // just the first member in branch should ev be doubbel ended
                            }
                            let left_start_contact = InOutContact {
                                end_point: arc_end.clone(),
                                start_point: left_start_xy,
                                subq: bidirect_arc,
                            };
                            singel_and_left_and_right_result.push(left_start_contact);
                        }
                        LayOut::_FirstEventTop => {}
                        LayOut::_FirstEventBottom => {}
                    }
                }
            }
            None => {}
        }
        match LAYOUTORDER {
            LayOut::_FirstEventRight => {
                for i in 0..self.singels.len() - 1 {
                    let right_contact = self.singels[i].get_center_right_contact();

                    let left_contact = self.singels[i + 1].get_center_left_contact();

                    singel_and_left_and_right_result.push(InOutContact {
                        start_point: right_contact,
                        end_point: left_contact,
                        subq: false,
                    });
                }
            }
            LayOut::FirstEventLeft => {
                for i in 1..self.singels.len() {
                    let right_contact = self.singels[i].get_center_right_contact();

                    let left_contact = self.singels[i - 1].get_center_left_contact();
                    let bidirect_arc = false;
                    singel_and_left_and_right_result.push(InOutContact {
                        start_point: right_contact,
                        end_point: left_contact,
                        subq: bidirect_arc,
                    });
                }
            }
            LayOut::_FirstEventTop => {}
            LayOut::_FirstEventBottom => {}
        };
        singel_and_left_and_right_result
    }
    /*++++++++++++++++++++++++++++++++++++*/
    fn as_svg_text(&mut self, running_y: f32) -> (TknXposModel, TknYposPX, String) {
        let mut result: Vec<String> = Vec::new();
        let mut max_y: RowCountModel = running_y;
        let mut max_x: TknXposModel = 0;
        for a_box in &self.singels {
            let sqlstm = match &a_box.sqlstm {
                Some(sqlstm) => sqlstm.clone(),
                None => "no comment".to_string(),
            };
            let text = format!("{}. {}", a_box.out_rel_id, sqlstm);
            result.push(format!(
                "<text class='TEXT' x='{text_at_x}' y='{text_at_y}'>{text}</text>",
                text_at_x = 0,
                text_at_y = translate_model_y_to_px(max_y),
                text = text
            ));
            max_x = cmp::max(max_x, text.len() as TknXposModel);
            max_y = max_y + 1.0;
        }
        match self.branch {
            Some(ref mut a_set_of_opercahin) => {
                for i in 0..a_set_of_opercahin.len() {
                    let (next_x, next_y, the_svg) = a_set_of_opercahin[i].as_svg_text(max_y);
                    result.push(the_svg);
                    max_x = cmp::max(max_x, next_x);
                    max_y = next_y
                }
            }
            None => {}
        }
        (max_x, max_y, result.join("\n"))
    }
    /*---------------------------------------------------------------------*/
    fn as_svg_box(&mut self) -> String {
        let mut result: Vec<String> = Vec::new();
        for a_box in &self.singels {
            let leftcenter_x_px = a_box.x_model as f32 * X_TO_PIXELS;
            let leftcenter_y_px = self.y_real_center;
            let all_text_at_x =
                leftcenter_x_px + LEFT_AND_RIGHT_MARGIN_AS_NR_LETTERS as f32 * X_TO_PIXELS;
            let box_x = leftcenter_x_px;
            let box_y = leftcenter_y_px - Y_BOX_HEIGHT_NR_PX / 2.0; // - Y_TO_PIXELS/2.0;

            let left_contact = a_box.get_center_left_contact();
            let right_contact = a_box.get_center_right_contact();
            result.push(
            format!(
"<rect class='BOX' x='{box_x}' y='{box_y}' width='{rect_width}' height='{rect_height}'></rect>
<text class='TEXT' x='{id_at_x}' y='{id_at_y}'>{rel_id}</text>
<text class='TEXT' x='{kind_at_x}' y='{kind_at_y}'>{kind}</text>
<text class='TEXT' x='{name_at_x}' y='{name_at_y}'>{name}</text>
<text class='TEXT' x='{rows_at_x}' y='{rows_at_y}'>{rows}</text>
<text class='TEXT' x='{cols_at_x}' y='{cols_at_y}'>{cols}</text>
<text class='TEXT' x='{cputime_at_x}' y='{cputime_at_y}'>{cputime}</text>
<circle class='KONTAKT' cx='{ktkin_x}' cy='{ktkin_y}' r='{ktk_r}'></circle>
<circle class='KONTAKT' cx='{ktkut_x}' cy='{ktkut_y}' r='{ktk_r}'></circle>
",
    box_x = box_x,
    box_y = box_y,

    rect_width = (1+a_box.text_width-0) as f32 * X_TO_PIXELS,
    rect_height = Y_BOX_HEIGHT_NR_PX - Y_TO_PIXELS/2.0,

    id_at_x = all_text_at_x,
    id_at_y = box_y + 1.0* Y_TO_PIXELS, // on down from corner

    kind_at_x = all_text_at_x,
    kind_at_y = box_y+ 2.0* Y_TO_PIXELS,

    name_at_x = all_text_at_x,
    name_at_y = box_y + 3.0* Y_TO_PIXELS,

    rows_at_x = all_text_at_x,
    rows_at_y = box_y + 4.0* Y_TO_PIXELS,

    cols_at_x = all_text_at_x,
    cols_at_y = box_y + 5.0* Y_TO_PIXELS,

    cputime_at_x = all_text_at_x,
    cputime_at_y = box_y + 6.0* Y_TO_PIXELS,

    rel_id=a_box.out_rel_id,
    kind = a_box.operation,
    name = a_box.outtablename,
    rows= a_box.nrrows,
    cols=a_box.nrcols,
    cputime=a_box.cputime,
    ktkin_x=left_contact.x,
    ktkin_y=left_contact.y,

    ktk_r=KONTAKT_RADIE,
    ktkut_x=right_contact.x,
    ktkut_y=right_contact.y,
                )
            );
        }
        match self.branch {
            Some(ref mut a_set_of_opercahin) => {
                for i in 0..a_set_of_opercahin.len() {
                    result.push(a_set_of_opercahin[i].as_svg_box());
                }
            }
            None => {}
        }
        result.join("\n")
    }
}
/*---------------------------------------------------------------------*/
/*---------------------------------------------------------------------*/
fn build_from_log_explainers(start_oper_log: i32) -> OperChain {
    let mut this_oneoper = OperChain::new();
    let mut this_singels: Vec<OneSingel> = Vec::new();

    let mut this_child_nr_boxes_y: i32 = 1;
    let mut this_oper_log = logit_get_value(start_oper_log);

    fn build_one_singel(self_oper_log: &Box<OperLog>) -> OneSingel {
        let full_rel_id = format!("id: {}", self_oper_log.out_table_id);
        let full_name = format!("name: {}", self_oper_log.outtablename);
        let full_kind = format!("kind: {}", self_oper_log.operation.to_string());
        let full_cputime = format!("cpu : {} ms", self_oper_log.duration.as_fractional_millis());
        let full_rows = format!("rows: {}", self_oper_log.nrrows);
        let full_cols = format!("cols: {}", self_oper_log.nrcols);

        let listan = vec![
            full_rel_id.len(),
            full_name.len(),
            full_kind.len(),
            full_cputime.len(),
            full_rows.len(),
            full_cols.len(),
        ];
        let max_inner_len_tkn = *listan.iter().max().unwrap() as TknCountModel;
        let width_tkn: TknCountModel = LEFT_AND_RIGHT_MARGIN_AS_NR_LETTERS
            + max_inner_len_tkn
            + LEFT_AND_RIGHT_MARGIN_AS_NR_LETTERS
            + X_BOX_SPACING_NR_LETTERS;
        let one_singel = OneSingel {
            x_model: 0,
            y_model: 0.0,
            operation: full_kind,
            outtablename: full_name,
            cputime: full_cputime,
            text_width: max_inner_len_tkn,
            outer_width: width_tkn,
            nrrows: full_rows,
            nrcols: full_cols,
            out_rel_id: full_rel_id,
            sqlstm: self_oper_log.sqlstm.clone(),
            the_log_oper: self_oper_log.operation.clone(),
        };
        one_singel
    }
    let mut we_are_done = false;
    let mut the_final_branch: Vec<OperChain> = Vec::new();

    while !we_are_done {
        match this_oper_log.legs.len() {
            1 => {
                let intable_id = this_oper_log.legs[0].intable_id;
                this_singels.push(build_one_singel(&this_oper_log));
                we_are_done = match intable_id {
                    Some(earlier_log_id) => {
                        this_oper_log = logit_get_value(earlier_log_id);
                        false
                    }
                    None => true,
                };
            }
            _ => {
                this_singels.push(build_one_singel(&this_oper_log)); // self
                this_child_nr_boxes_y = 0;
                for one_single in &this_oper_log.legs {
                    let a_leg_obj =
                        build_from_log_explainers(one_single.intable_id.unwrap() as i32);
                    let a_leg_nr_boxes_y = a_leg_obj.child_model_nr_boxes_y;
                    this_child_nr_boxes_y = this_child_nr_boxes_y + a_leg_nr_boxes_y;
                    the_final_branch.push(a_leg_obj);
                }
                we_are_done = true;
            }
        }
    }
    this_oneoper.singels = this_singels;
    this_oneoper.child_model_nr_boxes_y = this_child_nr_boxes_y;
    if the_final_branch.len() > 0 {
        this_oneoper.branch = Some(the_final_branch)
    }
    this_oneoper
}
/*---------------------------------------------------------------------*/
fn translate_model_x_to_px(in_x: TknXposModel) -> TknXposPX {
    in_x as f32 * X_TO_PIXELS
}
/*---------------------------------------------------------------------*/
fn translate_model_y_to_px(in_y: TknYposModel) -> TknYposPX {
    in_y * Y_TO_PIXELS
}
/*---------------------------------------------------------------------*/
fn as_svg(the_last: &mut OperChain, mima_opers: MinMaxTkn, the_sql_stm: &str) -> String {
    /*++++++++++++++++++++++++++++++++++++*/
    let intro = |w, h| -> String {
        return format!(
            "<svg xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink'
    id='canvas' width='{}' height='{}' preserveAspectRatio='xMidYMid'>\n",
            w, h
        );
    };
    /*++++++++++++++++++++++++++++++++++++*/
    let styles = || -> String {
        return "<defs>
        <marker id='arrowend' markerWidth='10' markerHeight='7'
        refX='+8.0' refY='2.5' orient='auto'>
          <polygon points='0 0, 8 2.5, 0 5' />
        </marker>
        <marker id='arrowstart' markerWidth='10' markerHeight='7'
        refX='+8.0' refY='2.5' orient='auto-start-reverse'>
          <polygon points='0 0, 8 2.5, 0 5' />
        </marker>
            <style type='text/css'>\n
    .TEXT {font-size:13px;font-weight: normal;font-family:'Menlo';stroke:none;fill:black;}
    .BOX {fill:Khaki;stroke:blue;stroke-width:1.5;fill-opacity:0.1;stroke-opacity:0.4}
    .COVERBOX {fill:white;stroke:black;stroke-width:1.5;fill-opacity:0.1;stroke-opacity:0.4}
    .KONTAKT {stroke-width:1.5;stroke:black;fill:none}
    .K2K {stroke-width:1.5;stroke:black;fill:none}
    </style>
    </defs>\n"
            .to_string();
    };
    /*++++++++++++++++++++++++++++++++++++*/
    fn allopers(the_last: &mut OperChain) -> String {
        the_last.as_svg_box()
    }
    /*++++++++++++++++++++++++++++++++++++*/
    fn allcomments(the_last: &mut OperChain) -> (TknXposModel, TknYposPX, String) {
        the_last.as_svg_text(1.0)
    }
    /*++++++++++++++++++++++++++++++++++++*/
    fn allconnections(the_last: &mut OperChain) -> String {
        let all_contacts = the_last.gen_in_and_out_contact();
        //        let format_one = "<line class='K2K' x1='{}' y1='{}' x2='{}' y2='{}' marker-end='url(#arrowhead)'/>";
        let ut: Vec<String> = all_contacts
            .iter()
            .map(
                |InOutContact {
                     start_point: Point { x: inx, y: iny },
                     end_point: Point { x: utx, y: uty },
                     subq,
                 }| {
                     if *subq{
                        format!(
                            "<line class='K2K' x1='{}' y1='{}' x2='{}' y2='{}' marker-end='url(#arrowend)' marker-start='url(#arrowstart)'/>",
                            inx, iny,
                            utx, uty,
                        )
                     }else{
                        format!(
                            "<line class='K2K' x1='{}' y1='{}' x2='{}' y2='{}' marker-end='url(#arrowend)'/>",
                            inx, iny,
                            utx, uty,
                        )
                     }
                },
            )
            .collect();
        ut.join("\n")
    }
    /*++++++++++++++++++++++++++++++++++++*/
    let sql_stm_to_svg = |sql: Vec<String>| -> String {
        sql.iter().enumerate().map(|(rownr,row)|{
            format!(
                "<text class='TEXT' xml:space='preserve' x='{text_at_x}' y='{text_at_y}'>{text}</text>",
                text_at_x=0,
                text_at_y=rownr as f32 *Y_TO_PIXELS,
                text=row
            )
        }).collect::<Vec<String>>().join("\n")
    };
    /*++++++++++++++++++++++++++++++++++++*/
    let cover_rect = |view_width:f32, view_height:f32| -> String {
        format!(
        "<rect class='COVERBOX' x='{box_x}' y='{box_y}' width='{rect_width}' height='{rect_height}'></rect>",
        box_x=5,
        box_y=5,
        rect_width=view_width-10.0,
        rect_height=view_height-10.0
    )
    };
        /*++++++++++++++++++++++++++++++++++++*/
    //
    // STARt HERE
    //
    let allopers = allopers(the_last); // holds svg
    let allconnections = allconnections(the_last); // holds svg
    let (max_x_comment, max_y_comment, comments_svg) = allcomments(the_last);
    let rows_sql = the_sql_stm
        .split("\n")
        .map(|ref s| s.to_string())
        .collect::<Vec<String>>();
    let row_lens: Vec<usize> = rows_sql.iter().map(|s| s.len()).collect();
    let max_x_sql = *row_lens.iter().max().unwrap();
    let max_y_sql = row_lens.len();

    // 1. set up containers for values
    //
    let mut oper_svg_box = SvgBox {
        calculated_screen_pos: Point { x: 0.0, y: 0.0 },
        relative_offset: Point { x: 0.0, y: 0.0 },
        calculated_transfer_to_zero: Point { x: 0.0, y: 0.0 },
        calculated_bottom_left: Point { x: 0.0, y: 0.0 },
        calculated_bottom_right: Point { x: 0.0, y: 0.0 },
        the_content_attributes: GraphAttributes {
            topleft: Point {
                x: -translate_model_x_to_px(mima_opers.xmin_real),
                y: -mima_opers.ymin_real,
            },
            width: (mima_opers.xmax_real - mima_opers.xmin_real) as f32 * X_TO_PIXELS,
            height: mima_opers.ymax_real - mima_opers.ymin_real,
        },
    };
    let mut comment_svg_box = SvgBox {
        calculated_screen_pos: Point { x: 0.0, y: 0.0 },
        relative_offset: Point { x: 0.0, y: 0.0 },
        calculated_transfer_to_zero: Point { x: 0.0, y: 0.0 },
        calculated_bottom_left: Point { x: 0.0, y: 0.0 },
        calculated_bottom_right: Point { x: 0.0, y: 0.0 },
        the_content_attributes: GraphAttributes {
            topleft: Point {
                x: -translate_model_x_to_px(0),
                y: -translate_model_y_to_px(0.0),
            },
            width: max_x_comment as f32 * X_TO_PIXELS,
            height: (max_y_comment + 0.0) * Y_TO_PIXELS,
        },
    };
    let mut sql_svg_box = SvgBox {
        calculated_screen_pos: Point { x: 0.0, y: 0.0 },
        relative_offset: Point { x: 0.0, y: 0.0 },
        calculated_transfer_to_zero: Point { x: 0.0, y: 0.0 },
        calculated_bottom_left: Point { x: 0.0, y: 0.0 },
        calculated_bottom_right: Point { x: 0.0, y: 0.0 },
        the_content_attributes: GraphAttributes {
            topleft: Point { x: 0.0, y: 0.0 },
            width: max_x_sql as f32 * X_TO_PIXELS,
            height: (max_y_sql as f32 + 0.0) * Y_TO_PIXELS,
        },
    };
    // 2. Make SVG transformation of each svg group so that topleft will be (0,0)
    //
    oper_svg_box.calculated_transfer_to_zero = oper_svg_box.the_content_attributes.topleft.clone();
    comment_svg_box.calculated_transfer_to_zero =
        comment_svg_box.the_content_attributes.topleft.clone();

    // 3. Locate on the screen with some margins
    oper_svg_box.relative_offset = Point { x: OPRSVG_RELATIVE_OFFSET_X, y: OPRSVG_RELATIVE_OFFSET_Y };
    comment_svg_box.relative_offset = Point { x: 0.0, y: BOX_SPACING_Y };
    sql_svg_box.relative_offset = Point { x: 0.0, y: BOX_SPACING_Y };
    //
    // 4. calculate screen coords
    //
    // 4.1 oper
    oper_svg_box.calculated_screen_pos = oper_svg_box.relative_offset.clone();

    oper_svg_box.calculated_bottom_left = oper_svg_box.calculated_screen_pos.add(Point {
        x: 0.0,
        y: oper_svg_box.the_content_attributes.height,
    });
    oper_svg_box.calculated_bottom_right = oper_svg_box.calculated_bottom_left.add(Point {
        x: oper_svg_box.the_content_attributes.width,
        y: 0.0,
    });
    //
    // 4.2 comment
    //
    comment_svg_box.calculated_screen_pos = oper_svg_box
        .calculated_bottom_left
        .add(comment_svg_box.relative_offset.clone());
    comment_svg_box.calculated_bottom_left = comment_svg_box.calculated_screen_pos.add(Point {
        x: 0.0,
        y: comment_svg_box.the_content_attributes.height,
    });
    comment_svg_box.calculated_bottom_right = comment_svg_box.calculated_bottom_left.add(Point {
        x: comment_svg_box.the_content_attributes.width,
        y: 0.0,
    });

    //
    // 4.2 sql statement
    //
    sql_svg_box.calculated_screen_pos = comment_svg_box
        .calculated_bottom_left
        .add(sql_svg_box.relative_offset.clone());
    sql_svg_box.calculated_bottom_left = sql_svg_box.calculated_screen_pos.add(Point {
        x: 0.0,
        y: sql_svg_box.the_content_attributes.height,
    });
    sql_svg_box.calculated_bottom_right = sql_svg_box.calculated_bottom_left.add(Point {
        x: sql_svg_box.the_content_attributes.width,
        y: 0.0,
    });

    //
    // 5. Calculate the total view dimensions
    //
    let view_height = oper_svg_box.relative_offset.y
        + oper_svg_box.the_content_attributes.height
        + comment_svg_box.relative_offset.y
        + comment_svg_box.the_content_attributes.height
        + sql_svg_box.relative_offset.y
        + sql_svg_box.the_content_attributes.height;

    let mut view_width = f32::MIN;
    for awidth in vec![
        oper_svg_box.relative_offset.x + oper_svg_box.the_content_attributes.width,
        comment_svg_box.relative_offset.x + comment_svg_box.the_content_attributes.width,
        sql_svg_box.relative_offset.x + sql_svg_box.the_content_attributes.width,
    ] {
        if awidth > view_width {
            view_width = awidth;
        }
    }
    let total = format!(
        "{intro}{styles}
{cover}
<g transform='translate({pos_opr_x},{pos_opr_y})'>
    <g transform='translate({opr_z_x},{opr_z_y})'>
    {allopers}{allconnections}
    </g>
</g>
<g transform='translate({pos_com_x},{pos_com_y})'>
    <g transform='translate({com_z_x},{com_z_y})'>
    {alltexts}
    </g>
</g>
<g transform='translate({pos_sql_x},{pos_sql_y})'>
    <g transform='translate({sql_z_x},{sql_z_y})'>
    {allsql}
    </g>
</g>
</svg>\n",
        intro = intro(view_width, view_height),
        styles = styles(),
        cover=cover_rect(view_width, view_height),
        pos_opr_x = oper_svg_box.calculated_screen_pos.x,
        pos_opr_y = oper_svg_box.calculated_screen_pos.y,
        opr_z_x = oper_svg_box.calculated_transfer_to_zero.x,
        opr_z_y = oper_svg_box.calculated_transfer_to_zero.y,
        pos_com_x = comment_svg_box.calculated_screen_pos.x,
        pos_com_y = comment_svg_box.calculated_screen_pos.y,
        pos_sql_x = sql_svg_box.calculated_screen_pos.x,
        pos_sql_y = sql_svg_box.calculated_screen_pos.y,
        allopers = allopers,
        allconnections = allconnections,
        com_z_x = comment_svg_box.calculated_transfer_to_zero.x,
        com_z_y = comment_svg_box.calculated_transfer_to_zero.y,
        sql_z_x = sql_svg_box.calculated_transfer_to_zero.x,
        sql_z_y = sql_svg_box.calculated_transfer_to_zero.y,
        alltexts = comments_svg,
        allsql = sql_stm_to_svg(rows_sql)
    );
    total
}
/*---------------------------------------------------------------------*/
pub fn explain(last_relat: &Table, the_sql_stm: &str) {
    //
    // START HERE
    //
    if is_logit_enabled() == false {
        return;
    }
    let last_log_id = last_relat.get_id();
    let mut slutet = build_from_log_explainers(last_log_id);
    slutet.calc_y_pos(0.0);
    slutet.calc_x_pos(0);
    let mut final_mima = slutet.calc_mima(MinMaxTkn {
        xmin_real: TknXposModel::MAX,
        xmax_real: i32::MIN,
        ymin_real: f32::MAX,
        ymax_real: f32::MIN,
    });
    match LAYOUTORDER {
        LayOut::_FirstEventRight | LayOut::FirstEventLeft => {
            final_mima.ymin_real = final_mima.ymin_real - Y_BOX_HEIGHT_NR_PX / 2.0;
            final_mima.ymax_real = final_mima.ymax_real + Y_BOX_HEIGHT_NR_PX / 2.0;
            final_mima.xmin_real = final_mima.xmin_real - 2; // two signs width;
            final_mima.xmax_real = final_mima.xmax_real + 2; // * KONTAKT_RADIE;
        }
        LayOut::_FirstEventTop => {}
        LayOut::_FirstEventBottom => {}
    }
    println!("{}", as_svg(&mut slutet, final_mima, the_sql_stm))
}
