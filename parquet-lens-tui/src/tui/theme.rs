use ratatui::style::Color;

pub struct Theme {
    pub bg: Color,
    pub fg: Color,
    pub highlight: Color,
    pub numeric: Color,
    pub string: Color,
    pub temporal: Color,
    pub boolean: Color,
    pub error: Color,
    pub warning: Color,
    pub success: Color,
}

impl Theme {
    pub fn dark() -> Self {
        Self {
            bg: Color::Black,
            fg: Color::White,
            highlight: Color::Yellow,
            numeric: Color::Cyan,
            string: Color::Green,
            temporal: Color::Yellow,
            boolean: Color::Magenta,
            error: Color::Red,
            warning: Color::LightYellow,
            success: Color::LightGreen,
        }
    }
    pub fn light() -> Self {
        Self {
            bg: Color::White,
            fg: Color::Black,
            highlight: Color::Blue,
            numeric: Color::DarkGray,
            string: Color::Green,
            temporal: Color::Yellow,
            boolean: Color::Magenta,
            error: Color::Red,
            warning: Color::LightYellow,
            success: Color::Green,
        }
    }
    pub fn nord() -> Self {
        Self {
            bg: Color::Rgb(46, 52, 64),
            fg: Color::Rgb(216, 222, 233),
            highlight: Color::Rgb(136, 192, 208),
            numeric: Color::Rgb(129, 161, 193),
            string: Color::Rgb(163, 190, 140),
            temporal: Color::Rgb(235, 203, 139),
            boolean: Color::Rgb(180, 142, 173),
            error: Color::Rgb(191, 97, 106),
            warning: Color::Rgb(235, 203, 139),
            success: Color::Rgb(163, 190, 140),
        }
    }
    pub fn catppuccin() -> Self {
        Self {
            bg: Color::Rgb(30, 30, 46),
            fg: Color::Rgb(205, 214, 244),
            highlight: Color::Rgb(137, 180, 250),
            numeric: Color::Rgb(137, 220, 235),
            string: Color::Rgb(166, 227, 161),
            temporal: Color::Rgb(249, 226, 175),
            boolean: Color::Rgb(203, 166, 247),
            error: Color::Rgb(243, 139, 168),
            warning: Color::Rgb(250, 179, 135),
            success: Color::Rgb(166, 227, 161),
        }
    }
    pub fn colorblind() -> Self {
        Self {
            bg: Color::Black,
            fg: Color::White,
            highlight: Color::Yellow,
            numeric: Color::Cyan,
            string: Color::Rgb(0x00, 0x80, 0xFF), // blue instead of green
            temporal: Color::Yellow,
            boolean: Color::Magenta,
            error: Color::Rgb(0xFF, 0x8C, 0x00),  // orange instead of red
            warning: Color::LightYellow,
            success: Color::Rgb(0x00, 0x80, 0xFF), // blue instead of green
        }
    }
    pub fn from_name(name: &str) -> Self {
        match name {
            "light" => Self::light(),
            "nord" => Self::nord(),
            "catppuccin" => Self::catppuccin(),
            "colorblind" => Self::colorblind(),
            _ => Self::dark(),
        }
    }
}
