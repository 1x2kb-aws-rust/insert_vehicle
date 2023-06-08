use serde::{Deserialize, Serialize};

#[cfg(test)]
use rand::{self, distributions::Alphanumeric, Rng};

#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Clone)]
pub struct Vehicle {
    pub make: String,
    pub model: String,
    #[serde(alias = "modelYear")]
    pub model_year: String,
    pub vin: String,
}

#[cfg(test)]
pub trait Random {
    fn random() -> Self;
}

#[cfg(test)]
impl Random for Vehicle {
    fn random() -> Self {
        let mut rng = rand::thread_rng();

        let alpha_numeric = |length: usize| {
            let mut rng = rand::thread_rng();

            (0..length)
                .map(|_| rng.sample(Alphanumeric) as char)
                .collect::<String>()
        };

        Vehicle {
            make: MAKES[rng.gen_range(0..MAKES.len())].to_string(),
            model: MODELS[rng.gen_range(0..MODELS.len())].to_string(),
            model_year: rng.gen_range(1..=2023).to_string(),
            vin: alpha_numeric(12),
        }
    }
}

#[cfg(test)]
const MAKES: &[&str] = &[
    "Chevrolet",
    "Alfa Romeo",
    "Dodge",
    "Kia",
    "BMW",
    "Audi",
    "Tesla",
    "Ford",
    "GMC",
    "Aston Martin",
    "Honda",
    "Mercedes-Benz",
    "Toyota",
    "Volkswagen",
    "Ferrari",
    "Chrysler",
    "Lexus",
    "Subaru",
    "Nissan",
    "Hyundai",
    "Jaguar",
    "Fiat",
    "Jeep",
    "Porsche",
    "HUMMER",
];

#[cfg(test)]
const MODELS: &[&str] = &[
    "Silverado",
    "SRX",
    "Routan",
    "Journey",
    "Paseo",
    "Colorado",
    "GTI",
    "XT5",
    "Legend",
    "Focus",
    "MKZ",
    "X1",
    "Q7",
    "Escape",
    "Optima",
    "S4",
    "RC",
    "C/K",
    "M5",
    "Enclave",
    "Monte Carlo",
    "Legacy",
    "Malibu",
    "S6",
    "Sentra",
];
