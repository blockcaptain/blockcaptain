use anyhow::{anyhow, Context as AnyhowContext, Result};
use comfy_table::presets::UTF8_FULL;
use comfy_table::*;
use libblkcapt::model::entities::FeatureState;
use presets::ASCII_NO_BORDERS;
use slog_scope::info;
use std::{error::Error, str::FromStr};
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

    info!(#"bc_raw", "{}", table);
}

pub fn comfy_feature_state_cell(state: FeatureState) -> Cell {
    Cell::new(state).fg(match state {
        FeatureState::Enabled => comfy_table::Color::Green,
        FeatureState::Paused => comfy_table::Color::Yellow,
        FeatureState::Unconfigured => comfy_table::Color::Red,
    })
}

pub fn comfy_id_header() -> Cell {
    comfy_identifier_header("ID")
}

pub fn comfy_index_header() -> Cell {
    comfy_identifier_header("Index")
}

pub fn comfy_identifier_header(name: &str) -> Cell {
    Cell::new(name).add_attribute(Attribute::Bold)
}

pub fn comfy_id_value(uuid: Uuid) -> Cell {
    Cell::new(&uuid.to_string()[0..8])
        .fg(Color::Blue)
        .add_attribute(Attribute::Bold)
}

pub fn comfy_id_value_full(uuid: Uuid) -> Cell {
    Cell::new(&uuid.to_string())
        .fg(Color::Blue)
        .add_attribute(Attribute::Bold)
}

pub fn comfy_name_value<T: ToString>(name: T) -> Cell {
    Cell::new(name).fg(Color::Blue)
}

pub enum CellOrCells {
    Cell(Cell),
    Cells(Vec<Cell>),
}

impl From<Cell> for CellOrCells {
    fn from(cell: Cell) -> Self {
        Self::Cell(cell)
    }
}

impl From<Vec<Cell>> for CellOrCells {
    fn from(cells: Vec<Cell>) -> Self {
        Self::Cells(cells)
    }
}

pub fn print_comfy_info(rows: Vec<(Cell, CellOrCells)>) {
    let mut table = Table::new();
    table
        .load_preset(ASCII_NO_BORDERS)
        .remove_style(TableComponent::HorizontalLines)
        .remove_style(TableComponent::VerticalLines)
        .remove_style(TableComponent::MiddleIntersections)
        .set_content_arrangement(ContentArrangement::Dynamic);

    for (header, value) in rows {
        match value {
            CellOrCells::Cell(cell) => {
                table.add_row(vec![header, cell]);
            }
            CellOrCells::Cells(cells) => {
                let mut cell_iter = cells.into_iter();
                table.add_row(vec![header, cell_iter.next().unwrap_or_else(|| Cell::new(""))]);
                cell_iter.for_each(|c| {
                    table.add_row(vec![Cell::new(""), c]);
                });
            }
        }
    }

    info!(#"bc_raw", "{}", table);
}

#[derive(Debug)]
pub struct UuidArg(Uuid);

impl UuidArg {
    pub fn uuid(&self) -> Uuid {
        self.0
    }

    pub fn parse(s: &str) -> Result<Uuid> {
        Self::from_str(s).map(|arg| arg.uuid())
    }
}

impl FromStr for UuidArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(UuidArg)
            .map_err(|e| e.source().map(|e| anyhow!(e.to_string())).unwrap_or(anyhow!(e)))
            .context(format!("'{}' is not a valid GUID", s))
    }
}
