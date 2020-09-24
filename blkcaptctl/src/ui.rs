use comfy_table::presets::UTF8_FULL;
use comfy_table::*;
use libblkcapt::model::entities::FeatureState;
use uuid::Uuid;

pub fn print_comfy_table(header: Vec<Cell>, rows: impl Iterator<Item = Vec<Cell>>) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(header);

    rows.for_each(|r| {
        table.add_row(r);
    });

    println!("{}", table);
}

pub fn comfy_feature_state_cell(state: FeatureState) -> Cell {
    Cell::new(state).fg(match state {
        FeatureState::Enabled => comfy_table::Color::Green,
        FeatureState::Paused => comfy_table::Color::Yellow,
        FeatureState::Unconfigured => comfy_table::Color::Red,
    })
}

pub fn comfy_id_header() -> Cell {
    Cell::new("ID").add_attribute(Attribute::Bold)
}

pub fn comfy_id_value(uuid: Uuid) -> Cell {
    Cell::new(&uuid.to_string()[0..8])
        .fg(Color::Blue)
        .add_attribute(Attribute::Bold)
}

pub fn comfy_name_value(name: &str) -> Cell {
    Cell::new(name).fg(Color::Blue)
}
