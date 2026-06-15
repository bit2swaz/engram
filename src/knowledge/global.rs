use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Visibility {
    Private,
    Shared,
}

impl Default for Visibility {
    fn default() -> Self {
        Visibility::Private
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_defaults_private_and_round_trips() {
        assert_eq!(Visibility::default(), Visibility::Private);
        let j = serde_json::to_string(&Visibility::Shared).unwrap();
        assert_eq!(serde_json::from_str::<Visibility>(&j).unwrap(), Visibility::Shared);
    }
}
